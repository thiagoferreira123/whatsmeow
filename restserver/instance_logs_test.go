package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types/events"
)

func TestInstanceLogsRetentionFilteringAndPagination(t *testing.T) {
	_, store := testPolicyManager(t, Config{})
	now := time.Now().UTC()
	entries := []InstanceLog{
		{InstanceID: "instance-1", Category: logCategoryConnection, Event: "old", Level: "info", CreatedAt: now.Add(-8 * 24 * time.Hour).Format(time.RFC3339Nano)},
		{InstanceID: "instance-1", Category: logCategoryConnection, Event: "connected", Level: "info", CreatedAt: now.Add(-time.Minute).Format(time.RFC3339Nano)},
		{InstanceID: "instance-1", Category: logCategorySend, Event: "send_success", Level: "info", Recipient: "5565999999999", MessageID: "msg-1", CreatedAt: now.Format(time.RFC3339Nano)},
	}
	for _, entry := range entries {
		if _, err := store.AddInstanceLog(entry); err != nil {
			t.Fatal(err)
		}
	}

	logs, err := store.ListInstanceLogs("instance-1", InstanceLogQuery{Limit: 1})
	if err != nil || len(logs) != 1 || logs[0].Event != "send_success" {
		t.Fatalf("first page = %#v, %v", logs, err)
	}
	next, err := store.ListInstanceLogs("instance-1", InstanceLogQuery{Limit: 10, BeforeID: logs[0].ID, Category: logCategoryConnection})
	if err != nil || len(next) != 2 || next[0].Event != "connected" {
		t.Fatalf("filtered page = %#v, %v", next, err)
	}

	deleted, err := store.CleanupInstanceLogs(7*24*time.Hour, now)
	if err != nil || deleted != 1 {
		t.Fatalf("cleanup deleted=%d err=%v", deleted, err)
	}
	remaining, _ := store.ListInstanceLogs("instance-1", InstanceLogQuery{Limit: 10})
	if len(remaining) != 2 {
		t.Fatalf("remaining logs=%d, want 2", len(remaining))
	}
}

func TestInstanceLogsCascadeWhenInstanceIsDeleted(t *testing.T) {
	_, store := testPolicyManager(t, Config{})
	if _, err := store.AddInstanceLog(InstanceLog{InstanceID: "instance-1", Category: logCategorySystem, Event: "created"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete("instance-1"); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM instance_logs WHERE instance_id='instance-1'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("logs after cascade=%d, want 0", count)
	}
}

func TestInstanceLogsHTTPIsAuthenticatedAndStructured(t *testing.T) {
	m, store := testPolicyManager(t, Config{})
	if _, err := store.AddInstanceLog(InstanceLog{
		InstanceID: "instance-1", Category: logCategoryConnection, Event: "connected", Level: "info", Status: "connected",
	}); err != nil {
		t.Fatal(err)
	}
	h := NewHandlers(m, Config{AdminAPIKey: "secret", InstanceLogRetentionDays: 7})

	unauthorized := httptest.NewRecorder()
	h.Router().ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/instances/instance-1/logs", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d", unauthorized.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/instances/instance-1/logs?category=connection&limit=10", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Logs          []InstanceLog `json:"logs"`
		RetentionDays int           `json:"retentionDays"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Logs) != 1 || body.Logs[0].Event != "connected" || body.RetentionDays != 7 {
		t.Fatalf("body=%#v", body)
	}
}

func TestInstanceLogNeverContainsMessageContentInPanelPayload(t *testing.T) {
	entry := InstanceLog{
		InstanceID: "instance-1", Category: logCategorySend, Event: "send_success", Level: "info",
		Recipient: "5565999999999", MessageType: "text", MessageID: "msg-1",
	}
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"token", "secret", "payload", "base64", "messageBody"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("audit payload contains forbidden field %q: %s", forbidden, data)
		}
	}
}

func TestConnectionAndQueueEventsAreAudited(t *testing.T) {
	m, store := testPolicyManager(t, Config{QueueMaxAttempts: 5})
	disconnectErr := errors.New("websocket: close 1006 abnormal closure")
	m.onDisconnected("instance-1", &events.Disconnected{Remote: true, Err: disconnectErr})
	job, created, err := m.EnqueueTextFrom("instance-1", "5565999999999", "conteúdo não auditado", "audit-key", "native_api")
	if err != nil || !created {
		t.Fatalf("enqueue=%#v created=%v err=%v", job, created, err)
	}
	logs, err := store.ListInstanceLogs("instance-1", InstanceLogQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 2 || logs[0].Event != "message_enqueued" || logs[1].Event != "socket_disconnected" {
		t.Fatalf("logs=%#v", logs)
	}
	if logs[1].Reason != disconnectErr.Error() || logs[1].Details["remote"] != true || logs[1].Details["errorType"] == "" {
		t.Fatalf("disconnect cause not preserved: %#v", logs[1])
	}
	encoded, _ := json.Marshal(logs)
	if strings.Contains(string(encoded), "conteúdo não auditado") {
		t.Fatalf("message body leaked into logs: %s", encoded)
	}
}

func TestInstanceLogRedactsSecretsAndSignedURLs(t *testing.T) {
	_, store := testPolicyManager(t, Config{})
	secretBlob := strings.Repeat("A", 120)
	_, err := store.AddInstanceLog(InstanceLog{
		InstanceID: "instance-1", Category: logCategorySend, Event: "send_failed", Level: "error",
		Reason: "download https://files.example.test/media?signature=private token=super-secret payload=" + secretBlob,
	})
	if err != nil {
		t.Fatal(err)
	}
	logs, err := store.ListInstanceLogs("instance-1", InstanceLogQuery{Limit: 1})
	if err != nil || len(logs) != 1 {
		t.Fatalf("logs=%#v err=%v", logs, err)
	}
	reason := logs[0].Reason
	for _, forbidden := range []string{"signature=private", "super-secret", secretBlob} {
		if strings.Contains(reason, forbidden) {
			t.Fatalf("reason leaked %q: %s", forbidden, reason)
		}
	}
}

func TestFailedSendIsAuditedWithoutMessageBody(t *testing.T) {
	m, store := testPolicyManager(t, Config{})
	_, err := m.SendText(withSendAudit(context.Background(), "native_api", ""), "instance-1", "5565999999999", "mensagem privada")
	if err == nil {
		t.Fatal("disconnected send unexpectedly succeeded")
	}
	logs, err := store.ListInstanceLogs("instance-1", InstanceLogQuery{Limit: 10, Category: logCategorySend})
	if err != nil || len(logs) != 2 {
		t.Fatalf("logs=%#v err=%v", logs, err)
	}
	if logs[0].Event != "send_failed" || logs[1].Event != "send_attempt" {
		t.Fatalf("unexpected send audit order: %#v", logs)
	}
	if logs[0].Recipient != "5565999999999" || logs[0].Reason == "" {
		t.Fatalf("failed recipient or cause missing: %#v", logs[0])
	}
	if logs[0].Details["attemptedRecipient"] != "5565999999999" || logs[0].Details["errorType"] == "" {
		t.Fatalf("failed send diagnostic details missing: %#v", logs[0].Details)
	}
	data, _ := json.Marshal(logs)
	if strings.Contains(string(data), "mensagem privada") {
		t.Fatalf("message body leaked: %s", data)
	}
}
