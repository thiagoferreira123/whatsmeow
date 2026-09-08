package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

const panelEditSchema = `CREATE TABLE IF NOT EXISTS panel_sent_text (
 instance_id TEXT NOT NULL,msg_id TEXT NOT NULL,chat TEXT NOT NULL,sent_at INTEGER NOT NULL,
 text_hash TEXT NOT NULL,edited_at INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(instance_id,msg_id));
 CREATE TABLE IF NOT EXISTS panel_edit_requests (
 instance_id TEXT NOT NULL,request_id TEXT NOT NULL,msg_id TEXT NOT NULL,payload_hash TEXT NOT NULL,
 status TEXT NOT NULL,created_at INTEGER NOT NULL,edited_at INTEGER NOT NULL DEFAULT 0,new_hash TEXT NOT NULL,
 PRIMARY KEY(instance_id,request_id));
 CREATE UNIQUE INDEX IF NOT EXISTS panel_edit_inflight ON panel_edit_requests(instance_id,msg_id) WHERE status IN ('sending','uncertain');
 CREATE INDEX IF NOT EXISTS panel_sent_text_age ON panel_sent_text(sent_at);
 CREATE INDEX IF NOT EXISTS panel_edit_request_age ON panel_edit_requests(created_at);`

func panelTextHash(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

func (m *Manager) rememberPanelText(in Instance, chat types.JID, id, text string, at time.Time) {
	if in.Name != "agendamento_bot" || text == "" {
		return
	}
	if at.IsZero() {
		at = time.Now()
	}
	_, _ = m.store.db.Exec(`INSERT INTO panel_sent_text(instance_id,msg_id,chat,sent_at,text_hash) VALUES(?,?,?,?,?) ON CONFLICT DO NOTHING`, in.ID, id, chat.String(), at.UnixMilli(), panelTextHash(text))
}

// Edit protocol events update existing history, never become new customer turns.
func (m *Manager) panelMessageEdit(instanceID string, event *events.Message) bool {
	rt := m.get(instanceID)
	if rt == nil || rt.metaCopy().Name != "agendamento_bot" {
		return false
	}
	p := event.Message.GetProtocolMessage()
	if p.GetType() != waE2E.ProtocolMessage_MESSAGE_EDIT || p.GetEditedMessage() == nil {
		return false
	}
	id := p.GetKey().GetID()
	text := extractText(p.GetEditedMessage())
	at := time.UnixMilli(p.GetTimestampMS())
	if at.IsZero() || p.GetTimestampMS() <= 0 {
		at = event.Info.Timestamp
	}
	if id == "" || text == "" || historyMedia(p.GetEditedMessage()) != "" || at.After(time.Now().Add(5*time.Minute)) {
		return true
	}
	in := rt.metaCopy()
	if event.Info.IsFromMe {
		_, _ = m.store.db.Exec(`UPDATE panel_sent_text SET text_hash=?,edited_at=? WHERE instance_id=? AND msg_id=? AND edited_at<?`, panelTextHash(text), at.UnixMilli(), in.ID, id, at.UnixMilli())
		_, _ = m.store.db.Exec(`UPDATE panel_edit_requests SET status='sent',edited_at=? WHERE instance_id=? AND msg_id=? AND new_hash=? AND status IN ('sending','uncertain') AND created_at<=?`, at.UnixMilli(), in.ID, id, panelTextHash(text), at.UnixMilli())
	}
	m.panelRecord(in, historyRecord{Type: "edit", Chat: event.Info.Chat.String(), MsgID: id, FromMe: event.Info.IsFromMe, Text: text, Ts: at.UTC().Format(time.RFC3339Nano)})
	return true
}

type panelEditBody struct {
	ID           string `json:"id"`
	Text         string `json:"text"`
	RequestID    string `json:"request_id"`
	ExpectedHash string `json:"expected_hash"`
}

var panelEditID = regexp.MustCompile(`^[a-f0-9-]{36}$`)

func (h *Handlers) uzPanelEdit(w http.ResponseWriter, r *http.Request) {
	in, ok := h.panelInstance(w, r)
	if !ok {
		return
	}
	var body panelEditBody
	if !readJSON(w, r, &body) {
		return
	}
	body.Text = strings.TrimSpace(body.Text)
	if !panelEditID.MatchString(body.RequestID) || body.ID == "" || len(body.ID) > 200 || body.Text == "" || utf8.RuneCountInString(body.Text) > 6000 || len(body.ExpectedHash) != 64 {
		writeErr(w, 400, "invalid edit")
		return
	}
	payloadHash := panelTextHash(body.ID + ":" + body.ExpectedHash + ":" + body.Text)
	var status, previousHash string
	var editedAt int64
	err := h.mgr.store.db.QueryRow(`SELECT status,payload_hash,edited_at FROM panel_edit_requests WHERE instance_id=? AND request_id=?`, in.ID, body.RequestID).Scan(&status, &previousHash, &editedAt)
	if err == nil {
		if previousHash != payloadHash {
			writeErr(w, 409, "idempotency conflict")
			return
		}
		writeJSON(w, 200, map[string]any{"status": status, "edited_at": editedAt})
		return
	}
	if err != sql.ErrNoRows {
		writeErr(w, 503, "edit unavailable")
		return
	}
	var chat, hash string
	var sentAt int64
	err = h.mgr.store.db.QueryRow(`SELECT chat,sent_at,text_hash FROM panel_sent_text WHERE instance_id=? AND msg_id=?`, in.ID, body.ID).Scan(&chat, &sentAt, &hash)
	if err != nil {
		writeErr(w, 404, "message not sent by this device")
		return
	}
	if time.Since(time.UnixMilli(sentAt)) >= 15*time.Minute {
		writeErr(w, 410, "edit window expired")
		return
	}
	if hash != body.ExpectedHash {
		writeErr(w, 409, "message has changed")
		return
	}
	rt, err := h.mgr.requireLoggedIn(in.ID)
	if err != nil {
		writeErr(w, 503, "instance unavailable")
		return
	}
	if until, parseErr := time.Parse(time.RFC3339, in.SendingBlockedUntil); parseErr == nil && time.Now().Before(until) {
		writeErr(w, 423, "instance temporarily blocked")
		return
	}
	jid, err := types.ParseJID(chat)
	if err != nil {
		writeErr(w, 409, "invalid stored contact")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	if err = h.mgr.acquireSendSlot(ctx); err != nil {
		writeErr(w, 503, "send slot unavailable")
		return
	}
	defer h.mgr.releaseSendSlot()
	tx, err := h.mgr.store.db.Begin()
	if err != nil {
		writeErr(w, 503, "edit unavailable")
		return
	}
	defer tx.Rollback()
	var recent int
	if err = tx.QueryRow(`SELECT count(*) FROM panel_edit_requests WHERE instance_id=? AND created_at>?`, in.ID, time.Now().Add(-time.Minute).UnixMilli()).Scan(&recent); err != nil {
		writeErr(w, 503, "edit unavailable")
		return
	}
	if recent >= 30 {
		w.Header().Set("Retry-After", "60")
		writeErr(w, 429, "edit rate limit")
		return
	}
	// Compare again inside the transaction; another device may have edited meanwhile.
	if err = tx.QueryRow(`SELECT text_hash FROM panel_sent_text WHERE instance_id=? AND msg_id=?`, in.ID, body.ID).Scan(&hash); err != nil || hash != body.ExpectedHash {
		writeErr(w, 409, "message has changed")
		return
	}
	if time.Since(time.UnixMilli(sentAt)) >= 15*time.Minute {
		writeErr(w, 410, "edit window expired")
		return
	}
	_, err = tx.Exec(`INSERT INTO panel_edit_requests(instance_id,request_id,msg_id,payload_hash,status,created_at,new_hash) VALUES(?,?,?,?,'sending',?,?)`, in.ID, body.RequestID, body.ID, payloadHash, time.Now().UnixMilli(), panelTextHash(body.Text))
	if err != nil {
		writeErr(w, 409, "edit already in progress")
		return
	}
	if err = tx.Commit(); err != nil {
		writeErr(w, 503, "edit unavailable")
		return
	}
	msg := rt.client.BuildEdit(jid, body.ID, &waE2E.Message{Conversation: proto.String(body.Text)})
	editedAt = msg.GetEditedMessage().GetMessage().GetProtocolMessage().GetTimestampMS()
	_, err = h.mgr.sendRecorded(ctx, rt, jid, msg)
	if err != nil {
		_, _ = h.mgr.store.db.Exec(`UPDATE panel_edit_requests SET status='uncertain' WHERE instance_id=? AND request_id=? AND status='sending'`, in.ID, body.RequestID)
		writeJSON(w, 200, map[string]any{"status": "uncertain"})
		return
	}
	_, err = h.mgr.store.db.Exec(`UPDATE panel_sent_text SET text_hash=?,edited_at=? WHERE instance_id=? AND msg_id=? AND edited_at<=?`, panelTextHash(body.Text), editedAt, in.ID, body.ID, editedAt)
	if err == nil {
		_, err = h.mgr.store.db.Exec(`UPDATE panel_edit_requests SET status='sent',edited_at=? WHERE instance_id=? AND request_id=?`, editedAt, in.ID, body.RequestID)
	}
	// Persisted idempotency record makes an interrupted HTTP response recoverable.
	if err != nil {
		writeErr(w, 503, "edit persistence unavailable")
		return
	}
	h.mgr.panelRecord(in, historyRecord{Type: "edit", Chat: chat, MsgID: body.ID, FromMe: true, Text: body.Text, Ts: time.UnixMilli(editedAt).UTC().Format(time.RFC3339Nano)})
	writeJSON(w, 200, map[string]any{"status": "sent", "edited_at": editedAt})
}
