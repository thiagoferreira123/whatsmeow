package main

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

const panelDeleteSchema = `CREATE TABLE IF NOT EXISTS panel_delete_requests (
 instance_id TEXT NOT NULL,request_id TEXT NOT NULL,msg_id TEXT NOT NULL,payload_hash TEXT NOT NULL,
 status TEXT NOT NULL,created_at INTEGER NOT NULL,deleted_at INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(instance_id,request_id));
 CREATE UNIQUE INDEX IF NOT EXISTS panel_delete_inflight ON panel_delete_requests(instance_id,msg_id) WHERE status IN ('sending','uncertain');
 CREATE INDEX IF NOT EXISTS panel_delete_request_age ON panel_delete_requests(created_at);`

// Revocations remove history on every device; they never become a new customer turn.
func (m *Manager) panelMessageDelete(instanceID string, event *events.Message) bool {
	rt := m.get(instanceID)
	if rt == nil || rt.metaCopy().Name != "agendamento_bot" {
		return false
	}
	p := event.Message.GetProtocolMessage()
	if p.GetType() != waE2E.ProtocolMessage_REVOKE {
		return false
	}
	id := p.GetKey().GetID()
	at := event.Info.Timestamp
	if id == "" || len(id) > 200 || at.IsZero() || at.After(time.Now().Add(5*time.Minute)) {
		return true
	}
	in := rt.metaCopy()
	// A message that no longer exists cannot be edited, and its text stops being remembered.
	_, _ = m.store.db.Exec(`DELETE FROM panel_sent_text WHERE instance_id=? AND msg_id=?`, in.ID, id)
	if event.Info.IsFromMe {
		// A real revoke echo also resolves an HTTP acknowledgement lost in transit.
		_, _ = m.store.db.Exec(`UPDATE panel_delete_requests SET status='sent',deleted_at=? WHERE instance_id=? AND msg_id=? AND status IN ('sending','uncertain')`,
			at.UnixMilli(), in.ID, id)
	}
	m.panelRecord(in, historyRecord{Type: "delete", Chat: event.Info.Chat.String(), MsgID: id, FromMe: event.Info.IsFromMe, Ts: at.UTC().Format(time.RFC3339Nano)})
	return true
}

type panelDeleteBody struct {
	Chat      string `json:"chat"`
	ID        string `json:"id"`
	RequestID string `json:"request_id"`
}

func (h *Handlers) uzPanelDelete(w http.ResponseWriter, r *http.Request) {
	in, ok := h.panelInstance(w, r)
	if !ok {
		return
	}
	var body panelDeleteBody
	if !readJSON(w, r, &body) {
		return
	}
	jid, err := types.ParseJID(body.Chat)
	if err != nil || (jid.Server != types.DefaultUserServer && jid.Server != types.HiddenUserServer) ||
		!panelEditID.MatchString(body.RequestID) || body.ID == "" || len(body.ID) > 200 {
		writeErr(w, 400, "invalid delete")
		return
	}
	payloadHash := panelTextHash(body.Chat + ":" + body.ID)
	var status, previousHash string
	var deletedAt int64
	err = h.mgr.store.db.QueryRow(`SELECT status,payload_hash,deleted_at FROM panel_delete_requests WHERE instance_id=? AND request_id=?`, in.ID, body.RequestID).Scan(&status, &previousHash, &deletedAt)
	if err == nil {
		if previousHash != payloadHash {
			writeErr(w, 409, "idempotency conflict")
			return
		}
		writeJSON(w, 200, map[string]any{"status": status, "deleted_at": deletedAt})
		return
	}
	if err != sql.ErrNoRows {
		writeErr(w, 503, "delete unavailable")
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
	var recent int
	if err = h.mgr.store.db.QueryRow(`SELECT count(*) FROM panel_delete_requests WHERE instance_id=? AND created_at>?`, in.ID, time.Now().Add(-time.Minute).UnixMilli()).Scan(&recent); err != nil {
		writeErr(w, 503, "delete unavailable")
		return
	}
	if recent >= 30 {
		w.Header().Set("Retry-After", "60")
		writeErr(w, 429, "delete rate limit")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	if err = h.mgr.acquireSendSlot(ctx); err != nil {
		writeErr(w, 503, "send slot unavailable")
		return
	}
	defer h.mgr.releaseSendSlot()
	// The partial unique index is what keeps a second revoke of the same message out.
	if _, err = h.mgr.store.db.Exec(`INSERT INTO panel_delete_requests(instance_id,request_id,msg_id,payload_hash,status,created_at) VALUES(?,?,?,?,'sending',?)`,
		in.ID, body.RequestID, body.ID, payloadHash, time.Now().UnixMilli()); err != nil {
		writeErr(w, 409, "delete already in progress")
		return
	}
	response, err := h.mgr.sendRecorded(ctx, rt, jid, rt.client.BuildRevoke(jid, types.EmptyJID, body.ID))
	if err != nil {
		_, _ = h.mgr.store.db.Exec(`UPDATE panel_delete_requests SET status='uncertain' WHERE instance_id=? AND request_id=? AND status='sending'`, in.ID, body.RequestID)
		writeJSON(w, 200, map[string]any{"status": "uncertain"})
		return
	}
	deletedAt = response.Timestamp.UnixMilli()
	if response.Timestamp.IsZero() {
		deletedAt = time.Now().UnixMilli()
	}
	_, _ = h.mgr.store.db.Exec(`DELETE FROM panel_sent_text WHERE instance_id=? AND msg_id=?`, in.ID, body.ID)
	// Persisted idempotency record makes an interrupted HTTP response recoverable.
	if _, err = h.mgr.store.db.Exec(`UPDATE panel_delete_requests SET status='sent',deleted_at=? WHERE instance_id=? AND request_id=?`, deletedAt, in.ID, body.RequestID); err != nil {
		writeErr(w, 503, "delete persistence unavailable")
		return
	}
	h.mgr.panelRecord(in, historyRecord{Type: "delete", Chat: body.Chat, MsgID: body.ID, FromMe: true, Ts: time.UnixMilli(deletedAt).UTC().Format(time.RFC3339Nano)})
	writeJSON(w, 200, map[string]any{"status": "sent", "deleted_at": deletedAt})
}
