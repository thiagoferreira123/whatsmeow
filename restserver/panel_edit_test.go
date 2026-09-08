package main

import (
	"bytes"
	"encoding/json"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPanelEditRejectsForeignTokenExpiredMessageAndConflict(t *testing.T) {
	m := testUazapiCompatManager(t, Config{AdminAPIKey: "synthetic-panel-key"})
	in := Instance{ID: "support", Name: "agendamento_bot", Token: "scoped-token"}
	_ = m.store.Create(&in)
	jid := types.NewJID("5511999999999", types.DefaultUserServer)
	m.rememberPanelText(in, jid, "old", "original", time.Now().Add(-16*time.Minute))
	m.rememberPanelText(in, jid, "new", "original", time.Now())
	h := NewHandlers(m, m.cfg)
	check := func(token, id, hash string, want int) {
		t.Helper()
		raw, _ := json.Marshal(panelEditBody{ID: id, Text: "edited", ExpectedHash: hash, RequestID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"})
		r := httptest.NewRequest("POST", "/instance/panel/edit", bytes.NewReader(raw))
		r.Header.Set("token", token)
		w := httptest.NewRecorder()
		h.uzPanelEdit(w, r)
		if w.Code != want {
			t.Fatalf("expected %d got %d: %s", want, w.Code, w.Body.String())
		}
	}
	check("wrong", "new", panelTextHash("original"), 401)
	check(in.Token, "unknown", panelTextHash("original"), 404)
	check(in.Token, "old", panelTextHash("original"), 410)
	check(in.Token, "new", panelTextHash("stale"), 409)
	other := Instance{ID: "other", Name: "another_instance", Token: "other-token"}
	_ = m.store.Create(&other)
	check(other.Token, "new", panelTextHash("original"), 403)
	// A retry returns a persisted success even after the deadline/device disconnect.
	body := panelEditBody{ID: "old", Text: "edited", ExpectedHash: panelTextHash("original"), RequestID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}
	_, err := m.store.db.Exec(`INSERT INTO panel_edit_requests(instance_id,request_id,msg_id,payload_hash,status,created_at,edited_at,new_hash) VALUES(?,?,?,?,'sent',?,?,?)`, in.ID, body.RequestID, body.ID, panelTextHash(body.ID+":"+body.ExpectedHash+":"+body.Text), time.Now().UnixMilli(), time.Now().UnixMilli(), panelTextHash(body.Text))
	if err != nil {
		t.Fatal(err)
	}
	check(in.Token, "old", body.ExpectedHash, 200)
}

func TestPanelEditProtocolUpdatesHistoryWithoutDeliveringAnotherMessage(t *testing.T) {
	m := testUazapiCompatManager(t, Config{})
	in := Instance{ID: "support", Name: "agendamento_bot"}
	m.runtimes[in.ID] = &instanceRuntime{meta: in}
	m.history = &historyHarvester{dir: t.TempDir(), targets: map[string]struct{}{"agendamento_bot": {}}}
	jid := types.NewJID("5511999999999", types.DefaultUserServer)
	at := time.Now()
	m.rememberPanelText(in, jid, "original-id", "original", at.Add(-time.Minute))
	edit := &events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: jid, IsFromMe: true}, ID: "edit-envelope", Timestamp: at}, Message: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(), Key: &waCommon.MessageKey{ID: proto.String("original-id")}, EditedMessage: &waE2E.Message{Conversation: proto.String("corrected")}, TimestampMS: proto.Int64(at.UnixMilli())}}}
	if !m.panelMessageEdit(in.ID, edit) {
		t.Fatal("edit not intercepted")
	}
	raw, err := os.ReadFile(filepath.Join(m.history.dir, "agendamento_bot.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var row historyRecord
	if json.Unmarshal(bytes.TrimSpace(raw), &row) != nil || row.Type != "edit" || row.MsgID != "original-id" || row.Text != "corrected" {
		t.Fatal("edit became a new message")
	}
	var hash string
	_ = m.store.db.QueryRow(`SELECT text_hash FROM panel_sent_text WHERE instance_id=? AND msg_id=?`, in.ID, "original-id").Scan(&hash)
	if hash != panelTextHash("corrected") {
		t.Fatal("edit not tracked")
	}
	m.runtimes["other"] = &instanceRuntime{meta: Instance{ID: "other", Name: "unrelated"}}
	if m.panelMessageEdit("other", edit) {
		t.Fatal("changed unrelated instance")
	}
}
