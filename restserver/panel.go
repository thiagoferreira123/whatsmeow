package main

// Private support-panel capabilities. Only agendamento_bot opts in; no change to
// the contracts, media retention or read receipts of other instances.
import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

const panelSchema = `CREATE TABLE IF NOT EXISTS panel_media (
 instance_id TEXT NOT NULL,msg_id TEXT NOT NULL,body BLOB NOT NULL,created_at INTEGER NOT NULL,
 PRIMARY KEY(instance_id,msg_id));
 CREATE INDEX IF NOT EXISTS panel_media_age ON panel_media(created_at);
 CREATE TABLE IF NOT EXISTS panel_resync (instance_id TEXT PRIMARY KEY,requested_at INTEGER NOT NULL);`
const panelMaxMedia = 32 << 20

func (h *Handlers) panelInstance(w http.ResponseWriter, r *http.Request) (Instance, bool) {
	in, ok := h.uzByToken(w, r)
	if !ok {
		return in, false
	}
	if in.Name != "agendamento_bot" {
		writeErr(w, 403, "support panel unavailable for this instance")
		return in, false
	}
	return in, true
}

func panelMedia(msg *waE2E.Message) (*waE2E.Message, string, uint64) {
	if msg == nil {
		return nil, "", 0
	}
	// View-once content remains view-once; never archive it in this panel.
	if msg.GetViewOnceMessage() != nil || msg.GetViewOnceMessageV2() != nil || msg.GetViewOnceMessageV2Extension() != nil {
		return nil, "", 0
	}
	if inner := msg.GetEphemeralMessage().GetMessage(); inner != nil {
		return panelMedia(inner)
	}
	switch {
	case msg.GetImageMessage() != nil:
		x := msg.GetImageMessage()
		return &waE2E.Message{ImageMessage: x}, x.GetMimetype(), x.GetFileLength()
	case msg.GetVideoMessage() != nil:
		x := msg.GetVideoMessage()
		return &waE2E.Message{VideoMessage: x}, x.GetMimetype(), x.GetFileLength()
	case msg.GetAudioMessage() != nil:
		x := msg.GetAudioMessage()
		return &waE2E.Message{AudioMessage: x}, x.GetMimetype(), x.GetFileLength()
	case msg.GetStickerMessage() != nil:
		x := msg.GetStickerMessage()
		return &waE2E.Message{StickerMessage: x}, x.GetMimetype(), x.GetFileLength()
	}
	return nil, "", 0
}

func (m *Manager) savePanelMedia(in Instance, id string, msg *waE2E.Message, at time.Time) bool {
	if in.Name != "agendamento_bot" || id == "" || at.Before(time.Now().Add(-90*24*time.Hour)) {
		return false
	}
	media, _, size := panelMedia(msg)
	if media == nil || size > panelMaxMedia {
		return false
	}
	raw, err := proto.Marshal(media)
	if err != nil {
		return false
	}
	aead, err := m.agentCipher()
	if err != nil {
		return false
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return false
	}
	encrypted := aead.Seal(nonce, nonce, raw, []byte(in.ID+":"+id))
	_, err = m.store.db.Exec(`INSERT INTO panel_media(instance_id,msg_id,body,created_at) VALUES(?,?,?,?)
 ON CONFLICT(instance_id,msg_id) DO UPDATE SET body=excluded.body`, in.ID, id, encrypted, at.Unix())
	return err == nil
}

func (m *Manager) panelRecord(in Instance, record historyRecord) {
	if in.Name != "agendamento_bot" || m.history == nil || !m.history.enabledFor(in) {
		return
	}
	h := m.history
	h.mu.Lock()
	defer h.mu.Unlock()
	if os.MkdirAll(h.dir, 0700) != nil {
		return
	}
	f, err := os.OpenFile(filepath.Join(h.dir, sanitizeHistoryName(in.Name)+".jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return
	}
	defer f.Close()
	_ = json.NewEncoder(f).Encode(record)
}

func (m *Manager) panelRead(instanceID string, evt any) {
	rt := m.get(instanceID)
	if rt == nil {
		return
	}
	in := rt.metaCopy()
	if in.Name != "agendamento_bot" {
		return
	}
	record := historyRecord{Type: "read"}
	switch v := evt.(type) {
	case *events.MarkChatAsRead:
		if v.JID.Server != types.DefaultUserServer && v.JID.Server != types.HiddenUserServer {
			return
		}
		read := v.Action.GetRead()
		record.Read = &read
		record.Chat = v.JID.String()
		record.Ts = v.Timestamp.UTC().Format(time.RFC3339Nano)
		through := v.Action.GetMessageRange().GetLastMessageTimestamp()
		if through > 0 {
			record.ReadThrough = time.Unix(through, 0).UTC().Format(time.RFC3339Nano)
		}
	case *events.Receipt:
		if v.Type != types.ReceiptTypeReadSelf || v.IsGroup {
			return
		}
		read := true
		record.Read = &read
		record.Chat = v.Chat.String()
		record.Ts = v.Timestamp.UTC().Format(time.RFC3339Nano)
		record.MessageIDs = v.MessageIDs
	default:
		return
	}
	m.panelRecord(in, record)
}

func (h *Handlers) uzPanelHistory(w http.ResponseWriter, r *http.Request) {
	in, ok := h.panelInstance(w, r)
	if !ok {
		return
	}
	_, _ = h.mgr.store.db.Exec(`DELETE FROM panel_media WHERE created_at<?`, time.Now().Add(-90*24*time.Hour).Unix())
	_, _ = h.mgr.store.db.Exec(`DELETE FROM panel_resync WHERE requested_at<?`, time.Now().Add(-48*time.Hour).Unix())
	offset, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	if err != nil || offset < 0 {
		writeErr(w, 400, "invalid cursor")
		return
	}
	f, err := os.Open(filepath.Join(h.mgr.history.dir, sanitizeHistoryName(in.Name)+".jsonl"))
	if err != nil {
		writeErr(w, 404, "history unavailable")
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		writeErr(w, 503, "history unavailable")
		return
	}
	reset := offset > info.Size()
	if reset {
		offset = 0
	}
	if _, err = f.Seek(offset, io.SeekStart); err != nil {
		writeErr(w, 400, "invalid cursor")
		return
	}
	reader := bufio.NewReader(io.LimitReader(f, 2<<20))
	items := []json.RawMessage{}
	next := offset
	for len(items) < 500 && next-offset < 1<<20 {
		line, readErr := reader.ReadBytes('\n')
		if readErr != nil {
			break
		}
		next += int64(len(line))
		if json.Valid(line) {
			items = append(items, json.RawMessage(line))
		}
	}
	writeJSON(w, 200, map[string]any{"items": items, "cursor": next, "more": next < info.Size(), "reset": reset})
}

func (h *Handlers) uzPanelMedia(w http.ResponseWriter, r *http.Request) {
	in, ok := h.panelInstance(w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	var encrypted []byte
	err := h.mgr.store.db.QueryRow(`SELECT body FROM panel_media WHERE instance_id=? AND msg_id=? AND created_at>?`, in.ID, id, time.Now().Add(-90*24*time.Hour).Unix()).Scan(&encrypted)
	if err != nil {
		writeErr(w, 404, "media reference unavailable")
		return
	}
	aead, err := h.mgr.agentCipher()
	if err != nil || len(encrypted) < aead.NonceSize() {
		writeErr(w, 503, "media unavailable")
		return
	}
	raw, err := aead.Open(nil, encrypted[:aead.NonceSize()], encrypted[aead.NonceSize():], []byte(in.ID+":"+id))
	if err != nil {
		writeErr(w, 503, "media unavailable")
		return
	}
	msg := &waE2E.Message{}
	if proto.Unmarshal(raw, msg) != nil {
		writeErr(w, 503, "media unavailable")
		return
	}
	media, mime, size := panelMedia(msg)
	if media == nil || size > panelMaxMedia {
		writeErr(w, 413, "media exceeds panel limit")
		return
	}
	rt, err := h.mgr.requireLoggedIn(in.ID)
	if err != nil {
		writeErr(w, 503, "instance unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	content, err := rt.client.DownloadAny(ctx, media)
	if err != nil {
		writeErr(w, 410, "media no longer available from WhatsApp")
		return
	}
	if len(content) > panelMaxMedia {
		writeErr(w, 413, "media exceeds panel limit")
		return
	}
	w.Header().Set("Content-Type", mime)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(200)
	_, _ = w.Write(content)
}

func (h *Handlers) uzPanelAvatar(w http.ResponseWriter, r *http.Request) {
	in, ok := h.panelInstance(w, r)
	if !ok {
		return
	}
	jid, err := types.ParseJID(r.URL.Query().Get("number"))
	if err != nil || (jid.Server != types.DefaultUserServer && jid.Server != types.HiddenUserServer) {
		writeErr(w, 400, "invalid contact")
		return
	}
	rt, err := h.mgr.requireLoggedIn(in.ID)
	if err != nil {
		writeErr(w, 503, "instance unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	pic, err := rt.client.GetProfilePictureInfo(ctx, jid, &whatsmeow.GetProfilePictureParams{Preview: true})
	if err != nil || pic == nil || pic.URL == "" {
		w.WriteHeader(204)
		return
	}
	u, err := url.Parse(pic.URL)
	if err != nil || u.Scheme != "https" || !(strings.HasSuffix(u.Hostname(), ".whatsapp.net") || strings.HasSuffix(u.Hostname(), ".fbcdn.net")) {
		writeErr(w, 502, "invalid photo host")
		return
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	client := &http.Client{Timeout: 8 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(req)
	if err != nil {
		writeErr(w, 503, "photo unavailable")
		return
	}
	defer response.Body.Close()
	content, err := io.ReadAll(io.LimitReader(response.Body, (512<<10)+1))
	if err != nil || response.StatusCode != 200 || len(content) > 512<<10 {
		writeErr(w, 503, "photo unavailable")
		return
	}
	mime := http.DetectContentType(content)
	if mime != "image/jpeg" && mime != "image/png" && mime != "image/webp" {
		writeErr(w, 502, "invalid photo")
		return
	}
	w.Header().Set("Content-Type", mime)
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(content)
}

func (h *Handlers) uzPanelRead(w http.ResponseWriter, r *http.Request) {
	in, ok := h.panelInstance(w, r)
	if !ok {
		return
	}
	var body struct {
		Chat      string `json:"chat"`
		ID        string `json:"id"`
		Timestamp int64  `json:"timestamp"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	jid, err := types.ParseJID(body.Chat)
	if err != nil || (jid.Server != types.DefaultUserServer && jid.Server != types.HiddenUserServer) || body.ID == "" || len(body.ID) > 200 || body.Timestamp <= 0 || body.Timestamp > time.Now().Add(time.Minute).Unix() {
		writeErr(w, 400, "invalid read marker")
		return
	}
	rt, err := h.mgr.requireLoggedIn(in.ID)
	if err != nil {
		writeErr(w, 503, "instance unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
	defer cancel()
	key := &waCommon.MessageKey{RemoteJID: proto.String(jid.String()), ID: proto.String(body.ID), FromMe: proto.Bool(false)}
	err = rt.client.SendAppState(ctx, appstate.BuildMarkChatAsRead(jid, true, time.Unix(body.Timestamp, 0), key))
	if err != nil {
		writeErr(w, 503, "WhatsApp did not confirm the read marker")
		return
	}
	// The app-state patch synchronizes linked devices. The receipt updates the sender.
	_ = rt.client.MarkRead(ctx, []types.MessageID{body.ID}, time.Now(), jid, jid)
	read := true
	h.mgr.panelRecord(in, historyRecord{Type: "read", Chat: body.Chat, Read: &read, MessageIDs: []string{body.ID}, ReadThrough: time.Unix(body.Timestamp, 0).UTC().Format(time.RFC3339Nano), Ts: time.Now().UTC().Format(time.RFC3339Nano)})
	writeJSON(w, 200, map[string]any{"read": true})
}

func (h *Handlers) uzPanelResync(w http.ResponseWriter, r *http.Request) {
	in, ok := h.panelInstance(w, r)
	if !ok {
		return
	}
	rt, err := h.mgr.requireLoggedIn(in.ID)
	if err != nil {
		writeErr(w, 503, "instance unavailable")
		return
	}
	result, err := h.mgr.store.db.Exec(`INSERT INTO panel_resync(instance_id,requested_at) VALUES(?,?)
 ON CONFLICT(instance_id) DO UPDATE SET requested_at=excluded.requested_at WHERE requested_at<?`, in.ID, time.Now().Unix(), time.Now().Add(-5*time.Minute).Unix())
	if err != nil {
		writeErr(w, 503, "sync unavailable")
		return
	}
	n, _ := result.RowsAffected()
	if n > 0 {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			err := rt.client.FetchAppState(ctx, appstate.WAPatchRegularLow, true, false)
			status := "complete"
			if err != nil {
				status = "retry"
			}
			h.mgr.panelRecord(in, historyRecord{Type: "read_sync", Text: status, Ts: time.Now().UTC().Format(time.RFC3339Nano)})
		}()
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"requested": n > 0})
}

// Ask the account's own phone for older messages; this never sends a chat message
// to a customer. Media recovery depends on the phone and WhatsApp retaining it.
func (h *Handlers) uzPanelBackfill(w http.ResponseWriter, r *http.Request) {
	in, ok := h.panelInstance(w, r)
	if !ok {
		return
	}
	var body struct {
		Chat      string `json:"chat"`
		ID        string `json:"id"`
		Timestamp int64  `json:"timestamp"`
		FromMe    bool   `json:"fromMe"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	jid, err := types.ParseJID(body.Chat)
	if err != nil || (jid.Server != types.DefaultUserServer && jid.Server != types.HiddenUserServer) || body.ID == "" || len(body.ID) > 200 || body.Timestamp <= 0 || body.Timestamp > time.Now().Add(time.Minute).Unix() {
		writeErr(w, 400, "invalid history boundary")
		return
	}
	rt, err := h.mgr.requireLoggedIn(in.ID)
	if err != nil {
		writeErr(w, 503, "instance unavailable")
		return
	}
	key := in.ID + ":history:" + jid.String()
	result, err := h.mgr.store.db.Exec(`INSERT INTO panel_resync(instance_id,requested_at) VALUES(?,?) ON CONFLICT(instance_id) DO UPDATE SET requested_at=excluded.requested_at WHERE requested_at<?`, key, time.Now().Unix(), time.Now().Add(-2*time.Minute).Unix())
	if err != nil {
		writeErr(w, 503, "history unavailable")
		return
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		writeJSON(w, 202, map[string]any{"requested": false})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
	defer cancel()
	msg := rt.client.BuildHistorySyncRequest(&types.MessageInfo{MessageSource: types.MessageSource{Chat: jid, IsFromMe: body.FromMe}, ID: body.ID, Timestamp: time.Unix(body.Timestamp, 0)}, 50)
	if _, err = rt.client.SendPeerMessage(ctx, msg); err != nil {
		writeErr(w, 503, "phone history request failed")
		return
	}
	writeJSON(w, 202, map[string]any{"requested": true})
}

// Initial historical read-state/media recovery, at most once a day. This asks
// the account's primary phone for its retained history without resetting pairing.
func (h *Handlers) uzPanelBootstrap(w http.ResponseWriter, r *http.Request) {
	in, ok := h.panelInstance(w, r)
	if !ok {
		return
	}
	rt, err := h.mgr.requireLoggedIn(in.ID)
	if err != nil {
		writeErr(w, 503, "instance unavailable")
		return
	}
	result, err := h.mgr.store.db.Exec(`INSERT INTO panel_resync(instance_id,requested_at) VALUES(?,?) ON CONFLICT(instance_id) DO UPDATE SET requested_at=excluded.requested_at WHERE requested_at<?`, in.ID+":bootstrap", time.Now().Unix(), time.Now().Add(-24*time.Hour).Unix())
	if err != nil {
		writeErr(w, 503, "history unavailable")
		return
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		writeJSON(w, 202, map[string]any{"requested": false})
		return
	}
	msg := &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
		Type: waE2E.ProtocolMessage_PEER_DATA_OPERATION_REQUEST_MESSAGE.Enum(),
		PeerDataOperationRequestMessage: &waE2E.PeerDataOperationRequestMessage{
			PeerDataOperationRequestType: waE2E.PeerDataOperationRequestType_FULL_HISTORY_SYNC_ON_DEMAND.Enum(),
			FullHistorySyncOnDemandRequest: &waE2E.PeerDataOperationRequestMessage_FullHistorySyncOnDemandRequest{
				RequestMetadata:               &waE2E.FullHistorySyncOnDemandRequestMetadata{RequestID: proto.String(rt.client.GenerateMessageID())},
				FullHistorySyncOnDemandConfig: &waE2E.FullHistorySyncOnDemandConfig{HistoryDurationDays: proto.Uint32(90), HistoryFromTimestamp: proto.Uint64(uint64(time.Now().Add(-90 * 24 * time.Hour).Unix()))},
			},
		},
	}}
	ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
	defer cancel()
	if _, err = rt.client.SendPeerMessage(ctx, msg); err != nil {
		writeErr(w, 503, "phone history request failed")
		return
	}
	writeJSON(w, 202, map[string]any{"requested": true})
}
