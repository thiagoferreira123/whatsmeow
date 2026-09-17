package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

const deleteRequestID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

func TestPanelDeleteRejectsForeignTokenInvalidTargetAndReplaysStoredResult(t *testing.T) {
	m := testUazapiCompatManager(t, Config{AdminAPIKey: "synthetic-panel-key"})
	in := Instance{ID: "support", Name: "agendamento_bot", Token: "scoped-token"}
	_ = m.store.Create(&in)
	h := NewHandlers(m, m.cfg)
	chat := types.NewJID("5511999999999", types.DefaultUserServer).String()
	check := func(token, chat, id, request string, want int) {
		t.Helper()
		raw, _ := json.Marshal(panelDeleteBody{Chat: chat, ID: id, RequestID: request})
		r := httptest.NewRequest("POST", "/instance/panel/delete", bytes.NewReader(raw))
		r.Header.Set("token", token)
		w := httptest.NewRecorder()
		h.uzPanelDelete(w, r)
		if w.Code != want {
			t.Fatalf("expected %d got %d: %s", want, w.Code, w.Body.String())
		}
	}
	check("wrong", chat, "target", deleteRequestID, 401)
	check(in.Token, "5511999999999@g.us", "target", deleteRequestID, 400)
	check(in.Token, chat, "", deleteRequestID, 400)
	check(in.Token, chat, "target", "not-a-uuid", 400)
	other := Instance{ID: "other", Name: "another_instance", Token: "other-token"}
	_ = m.store.Create(&other)
	check(other.Token, chat, "target", deleteRequestID, 403)
	// A persisted outcome answers a retry without asking WhatsApp to revoke twice.
	_, err := m.store.db.Exec(`INSERT INTO panel_delete_requests(instance_id,request_id,msg_id,payload_hash,status,created_at,deleted_at) VALUES(?,?,?,?,'sent',?,?)`,
		in.ID, deleteRequestID, "target", panelTextHash(chat+":"+"target"), time.Now().UnixMilli(), time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	check(in.Token, chat, "target", deleteRequestID, 200)
	// The same request id pointing at another message is a client bug, never a second revoke.
	check(in.Token, chat, "another-target", deleteRequestID, 409)
}

func TestPanelRevokeUpdatesHistoryWithoutBecomingANewMessage(t *testing.T) {
	m := testUazapiCompatManager(t, Config{})
	in := Instance{ID: "support", Name: "agendamento_bot"}
	m.runtimes[in.ID] = &instanceRuntime{meta: in}
	m.history = &historyHarvester{dir: t.TempDir(), targets: map[string]struct{}{"agendamento_bot": {}}}
	jid := types.NewJID("5511999999999", types.DefaultUserServer)
	at := time.Now()
	m.rememberPanelText(in, jid, "original-id", "original", at.Add(-time.Minute))
	if _, err := m.store.db.Exec(`INSERT INTO panel_delete_requests(instance_id,request_id,msg_id,payload_hash,status,created_at) VALUES(?,?,?,?,'uncertain',?)`,
		in.ID, deleteRequestID, "original-id", panelTextHash(jid.String()+":original-id"), at.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	revoke := &events.Message{
		Info:    types.MessageInfo{MessageSource: types.MessageSource{Chat: jid, IsFromMe: true}, ID: "revoke-envelope", Timestamp: at},
		Message: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_REVOKE.Enum(), Key: &waCommon.MessageKey{ID: proto.String("original-id")}}},
	}
	if !m.panelMessageDelete(in.ID, revoke) {
		t.Fatal("revoke not intercepted")
	}
	raw, err := os.ReadFile(filepath.Join(m.history.dir, "agendamento_bot.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var row historyRecord
	if json.Unmarshal(bytes.TrimSpace(raw), &row) != nil || row.Type != "delete" || row.MsgID != "original-id" || !row.FromMe {
		t.Fatalf("revoke became a new message: %s", raw)
	}
	var remembered int
	_ = m.store.db.QueryRow(`SELECT count(*) FROM panel_sent_text WHERE instance_id=? AND msg_id=?`, in.ID, "original-id").Scan(&remembered)
	if remembered != 0 {
		t.Fatal("a deleted message must not stay editable")
	}
	var status string
	_ = m.store.db.QueryRow(`SELECT status FROM panel_delete_requests WHERE instance_id=? AND request_id=?`, in.ID, deleteRequestID).Scan(&status)
	if status != "sent" {
		t.Fatalf("the echo must confirm the pending revoke, got %q", status)
	}
	m.runtimes["other"] = &instanceRuntime{meta: Instance{ID: "other", Name: "unrelated"}}
	if m.panelMessageDelete("other", revoke) {
		t.Fatal("changed unrelated instance")
	}
}

func TestPanelIncomingRevokeIsRecordedWithoutTouchingRequests(t *testing.T) {
	m := testUazapiCompatManager(t, Config{})
	in := Instance{ID: "support", Name: "agendamento_bot"}
	m.runtimes[in.ID] = &instanceRuntime{meta: in}
	m.history = &historyHarvester{dir: t.TempDir(), targets: map[string]struct{}{"agendamento_bot": {}}}
	jid := types.NewJID("5511999999999", types.DefaultUserServer)
	at := time.Now()
	revoke := &events.Message{
		Info:    types.MessageInfo{MessageSource: types.MessageSource{Chat: jid, IsFromMe: false}, ID: "revoke-envelope", Timestamp: at},
		Message: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_REVOKE.Enum(), Key: &waCommon.MessageKey{ID: proto.String("customer-id")}}},
	}
	if !m.panelMessageDelete(in.ID, revoke) {
		t.Fatal("customer revoke not intercepted")
	}
	raw, err := os.ReadFile(filepath.Join(m.history.dir, "agendamento_bot.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var row historyRecord
	if json.Unmarshal(bytes.TrimSpace(raw), &row) != nil || row.Type != "delete" || row.MsgID != "customer-id" || row.FromMe {
		t.Fatalf("customer revoke recorded wrong: %s", raw)
	}
}
