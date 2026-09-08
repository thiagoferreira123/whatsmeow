package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	waLog "go.mau.fi/whatsmeow/util/log"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAgentWebhookDurableSignedAndDeduplicated(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		body, _ := io.ReadAll(r.Body)
		digest := sha256.Sum256(body)
		canonical := strings.Join([]string{"POST", r.URL.Path, r.Header.Get("x-agents-timestamp"), r.Header.Get("x-agents-nonce"), hex.EncodeToString(digest[:])}, "\n")
		mac := hmac.New(sha256.New, []byte("synthetic-hook"))
		mac.Write([]byte(canonical))
		if hex.EncodeToString(mac.Sum(nil)) != r.Header.Get("x-agents-signature") {
			t.Error("invalid signature")
		}
		if strings.Contains(string(body), "must-not-persist") {
			t.Error("token leaked")
		}
		if len(r.Header.Get("x-agents-nonce")) != 32 {
			t.Error("nonce missing")
		}
		w.WriteHeader(202)
	}))
	defer server.Close()
	m, store := testPolicyManager(t, Config{AdminAPIKey: "synthetic-master", WebhookSecret: "synthetic-hook"})
	m.log = waLog.Noop
	m.webhooks = NewWebhookSender()
	m.webhooks.retryBackoff = func(int) time.Duration { return 0 }
	in := m.runtimes["instance-1"].meta
	in.Name = "agendamento_bot"
	in.WebhookURL = server.URL + "/v1/whatsmeow"
	in.WebhookEnabled = true
	m.runtimes["instance-1"].meta = in
	payload := map[string]any{"instanceName": "agendamento_bot", "token": "must-not-persist", "instance": map[string]any{"id": in.ID, "token": "must-not-persist"}, "message": map[string]any{"messageid": "one", "text": "question"}}
	if !m.enqueueAgentWebhook(in.WebhookURL, payload) || !m.enqueueAgentWebhook(in.WebhookURL, payload) {
		t.Fatal("not persisted")
	}
	var n int
	store.db.QueryRow("SELECT count(*) FROM agent_webhook_outbox").Scan(&n)
	if n != 1 {
		t.Fatal("duplicate queued")
	}
	// New sender object simulates a process restart; queue lives in the database.
	m.webhooks = NewWebhookSender()
	if !m.dispatchAgentWebhook() {
		t.Fatal("not dispatched")
	}
	if m.dispatchAgentWebhook() || calls != 1 {
		t.Fatal("duplicate delivery")
	}
	var body []byte
	store.db.QueryRow("SELECT body FROM agent_webhook_outbox").Scan(&body)
	if body != nil {
		t.Fatal("payload retained after ack")
	}
	if m.enqueueAgentWebhook("https://another.example/hook", payload) {
		t.Fatal("changed legacy webhook")
	}
}
