package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestPanelMediaIsEncryptedScopedAndExcludesViewOnce(t *testing.T) {
	m := testUazapiCompatManager(t, Config{AdminAPIKey: "synthetic-panel-key"})
	in := Instance{ID: "support", Name: "agendamento_bot"}
	msg := &waE2E.Message{ImageMessage: &waE2E.ImageMessage{URL: proto.String("https://example.invalid/private"), Mimetype: proto.String("image/jpeg"), FileLength: proto.Uint64(100)}}
	if !m.savePanelMedia(in, "media-one", msg, time.Now()) {
		t.Fatal("media was not stored")
	}
	var raw []byte
	if err := m.store.db.QueryRow(`SELECT body FROM panel_media WHERE instance_id=? AND msg_id=?`, in.ID, "media-one").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("private")) {
		t.Fatal("plaintext media reference")
	}
	aead, _ := m.agentCipher()
	if _, err := aead.Open(nil, raw[:aead.NonceSize()], raw[aead.NonceSize():], []byte("other:media-one")); err == nil {
		t.Fatal("cross-instance ciphertext accepted")
	}
	if m.savePanelMedia(Instance{ID: "other", Name: "nutricionist_other"}, "x", msg, time.Now()) {
		t.Fatal("unrelated instance captured")
	}
	if m.savePanelMedia(in, "view-once", &waE2E.Message{ViewOnceMessageV2: &waE2E.FutureProofMessage{Message: msg}}, time.Now()) {
		t.Fatal("view-once archived")
	}
	msg.ImageMessage.FileLength = proto.Uint64(panelMaxMedia + 1)
	if m.savePanelMedia(in, "large", msg, time.Now()) {
		t.Fatal("oversize media accepted")
	}
}

func TestPanelIncrementalHistoryDoesNotLosePartialLines(t *testing.T) {
	m := testUazapiCompatManager(t, Config{AdminAPIKey: "synthetic-panel-key"})
	in := Instance{ID: "support", Name: "agendamento_bot", Token: "scoped-token"}
	if err := m.store.Create(&in); err != nil {
		t.Fatal(err)
	}
	m.history = &historyHarvester{dir: t.TempDir(), targets: map[string]struct{}{"agendamento_bot": {}}}
	first := []byte("{\"type\":\"message\",\"msgId\":\"one\"}\n")
	path := filepath.Join(m.history.dir, "agendamento_bot.jsonl")
	if err := os.WriteFile(path, append(first, []byte("{\"type\":")...), 0600); err != nil {
		t.Fatal(err)
	}
	h := NewHandlers(m, m.cfg)
	request := httptest.NewRequest("GET", "/instance/panel/history?offset=0", nil)
	request.Header.Set("token", in.Token)
	out := httptest.NewRecorder()
	h.uzPanelHistory(out, request)
	var result struct {
		Items  []json.RawMessage `json:"items"`
		Cursor int               `json:"cursor"`
	}
	if out.Code != 200 || json.Unmarshal(out.Body.Bytes(), &result) != nil || len(result.Items) != 1 || result.Cursor != len(first) {
		t.Fatalf("bad cursor: %s", out.Body.String())
	}
	request.Header.Set("token", "wrong")
	out = httptest.NewRecorder()
	h.uzPanelHistory(out, request)
	if out.Code != 401 {
		t.Fatal("unauthenticated history accepted")
	}
	other := Instance{ID: "other", Name: "nutricionist_other", Token: "other-token"}
	_ = m.store.Create(&other)
	request.Header.Set("token", other.Token)
	out = httptest.NewRecorder()
	h.uzPanelHistory(out, request)
	if out.Code != 403 {
		t.Fatal("other instance can access support history")
	}
}

func TestPanelArchivesDocumentsIncludingTheCaptionWrapper(t *testing.T) {
	m := testUazapiCompatManager(t, Config{AdminAPIKey: "synthetic-panel-key"})
	in := Instance{ID: "support", Name: "agendamento_bot"}
	doc := &waE2E.DocumentMessage{URL: proto.String("https://example.invalid/plan"), Mimetype: proto.String("application/pdf"),
		FileName: proto.String("plano-alimentar.pdf"), FileLength: proto.Uint64(2048), Caption: proto.String("Seu plano")}
	plain := &waE2E.Message{DocumentMessage: doc}
	wrapped := &waE2E.Message{DocumentWithCaptionMessage: &waE2E.FutureProofMessage{Message: plain}}
	for name, msg := range map[string]*waE2E.Message{"plain": plain, "wrapped": wrapped} {
		media, mime, size := panelMedia(msg)
		if media.GetDocumentMessage() == nil || mime != "application/pdf" || size != 2048 {
			t.Fatalf("%s document not archived: mime %q size %d", name, mime, size)
		}
		if historyMedia(msg) != "document" {
			t.Fatalf("%s document not classified", name)
		}
		if historyText(msg) != "Seu plano" {
			t.Fatalf("%s document caption lost", name)
		}
		if panelMediaName(msg) != "plano-alimentar.pdf" {
			t.Fatalf("%s file name lost", name)
		}
	}
	if !m.savePanelMedia(in, "doc-one", wrapped, time.Now()) {
		t.Fatal("document reference not stored")
	}
	doc.FileLength = proto.Uint64(panelMaxMedia + 1)
	if m.savePanelMedia(in, "doc-large", plain, time.Now()) {
		t.Fatal("oversize document accepted")
	}
	if panelMediaName(&waE2E.Message{ImageMessage: &waE2E.ImageMessage{}}) != "" {
		t.Fatal("only documents carry a file name")
	}
}

func TestPanelReadMarkerDefaultsToReadAndSupportsUnread(t *testing.T) {
	var body panelReadBody
	if err := json.Unmarshal([]byte(`{"chat":"5511999999999@s.whatsapp.net","id":"one","timestamp":1}`), &body); err != nil {
		t.Fatal(err)
	}
	if !body.marksRead() {
		t.Fatal("a request without \"read\" must keep marking the chat as read")
	}
	if err := json.Unmarshal([]byte(`{"chat":"5511999999999@s.whatsapp.net","id":"one","timestamp":1,"read":false}`), &body); err != nil {
		t.Fatal(err)
	}
	if body.marksRead() {
		t.Fatal("read:false must mark the chat as unread")
	}
	jid := types.NewJID("5511999999999", types.DefaultUserServer)
	at := time.Unix(1700000000, 0)
	unread := panelReadPatch(jid, false, at, "one")
	if unread.Type != appstate.WAPatchRegularLow || len(unread.Mutations) != 1 {
		t.Fatal("unexpected patch shape")
	}
	if unread.Mutations[0].Value.GetMarkChatAsReadAction().GetRead() {
		t.Fatal("unread patch still marks the chat as read")
	}
	if index := unread.Mutations[0].Index; len(index) != 2 || index[0] != appstate.IndexMarkChatAsRead || index[1] != jid.String() {
		t.Fatalf("patch not scoped to the chat: %v", index)
	}
	if !panelReadPatch(jid, true, at, "one").Mutations[0].Value.GetMarkChatAsReadAction().GetRead() {
		t.Fatal("read patch regressed")
	}
}

func TestPanelReadEventsDistinguishCustomerReceiptsAndOurReading(t *testing.T) {
	m := testUazapiCompatManager(t, Config{})
	in := Instance{ID: "support", Name: "agendamento_bot"}
	m.runtimes[in.ID] = &instanceRuntime{meta: in}
	m.history = &historyHarvester{dir: t.TempDir(), targets: map[string]struct{}{"agendamento_bot": {}}}
	jid := types.NewJID("5511999999999", types.DefaultUserServer)
	received := &events.Receipt{MessageSource: types.MessageSource{Chat: jid}, Type: types.ReceiptTypeRead, MessageIDs: []string{"one"}, Timestamp: time.Now()}
	m.panelRead(in.ID, received)
	path := filepath.Join(m.history.dir, "agendamento_bot.jsonl")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("customer reading was confused with our unread inbox")
	}
	received.Type = types.ReceiptTypeReadSelf
	m.panelRead(in.ID, received)
	m.panelRead(in.ID, &events.MarkChatAsRead{JID: jid, Timestamp: time.Now(), Action: &waSyncAction.MarkChatAsReadAction{Read: proto.Bool(false)}})
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(content, []byte(`"read":true`)) || !bytes.Contains(content, []byte(`"read":false`)) {
		t.Fatal("missing read/unread events")
	}
}
