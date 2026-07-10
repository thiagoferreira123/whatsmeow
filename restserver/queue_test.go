package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestQueueIdempotencyAndPerInstanceSerialization(t *testing.T) {
	m, store := testPolicyManager(t, Config{QueueMaxAttempts: 5})
	first, created, err := m.EnqueueText("instance-1", "5565999999999", "one", "request-1")
	if err != nil || !created {
		t.Fatalf("first enqueue = %#v, %v, created=%v", first, err, created)
	}
	duplicate, created, err := m.EnqueueText("instance-1", "5565999999999", "one", "request-1")
	if err != nil || created || duplicate.ID != first.ID {
		t.Fatalf("duplicate enqueue = %#v, %v, created=%v", duplicate, err, created)
	}
	if _, _, err := m.EnqueueText("instance-1", "5565999999999", "different", "request-1"); apiStatus(err) != 409 {
		t.Fatalf("reused key with different payload = %v, want 409", err)
	}
	second, created, err := m.EnqueueText("instance-1", "5565888888888", "two", "request-2")
	if err != nil || !created {
		t.Fatal(err)
	}

	claimed, err := store.ClaimQueueJob(context.Background(), time.Now().Add(time.Second))
	if err != nil || claimed.ID != first.ID {
		t.Fatalf("first claim = %#v, %v", claimed, err)
	}
	if _, err := store.ClaimQueueJob(context.Background(), time.Now().Add(time.Second)); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("same instance must have one processing job, got %v", err)
	}
	if err := store.FinishQueue(first.ID, "wa-message", time.Now()); err != nil {
		t.Fatal(err)
	}
	claimed, err = store.ClaimQueueJob(context.Background(), time.Now().Add(time.Second))
	if err != nil || claimed.ID != second.ID {
		t.Fatalf("second claim = %#v, %v", claimed, err)
	}
}

func TestQueueRecoversProcessingJobsAfterRestart(t *testing.T) {
	m, store := testPolicyManager(t, Config{QueueMaxAttempts: 5})
	job, _, err := m.EnqueueText("instance-1", "5565999999999", "recover", "recover-key")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimQueueJob(context.Background(), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStore(store.db); err != nil {
		t.Fatal(err)
	}
	recovered, err := store.GetQueueByKey("instance-1", "recover-key")
	if err != nil || recovered.ID != job.ID || recovered.Status != queueQueued {
		t.Fatalf("recovered job = %#v, %v", recovered, err)
	}
}

func TestQueueWaitsForDisconnectedSessionWithoutBurningAttempt(t *testing.T) {
	m, store := testPolicyManager(t, Config{QueueMaxAttempts: 5})
	_, _, err := m.EnqueueText("instance-1", "5565999999999", "wait", "wait-key")
	if err != nil {
		t.Fatal(err)
	}
	job, err := store.ClaimQueueJob(context.Background(), time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	m.processQueueJob(context.Background(), job)
	waiting, err := store.GetQueueByKey("instance-1", "wait-key")
	if err != nil {
		t.Fatal(err)
	}
	if waiting.Status != queueWaitingConnection || waiting.Attempts != 0 {
		t.Fatalf("waiting job = %#v", waiting)
	}
}

func TestAsyncHTTPEnqueuesWithoutConnectedClient(t *testing.T) {
	m, store := testPolicyManager(t, Config{QueueMaxAttempts: 5})
	h := NewHandlers(m, Config{})
	body, _ := json.Marshal(map[string]any{
		"number": "5565999999999", "text": "queued", "async": true, "idempotencyKey": "http-key",
	})
	req := httptest.NewRequest(http.MethodPost, "/instances/instance-1/send/text", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	job, err := store.GetQueueByKey("instance-1", "http-key")
	if err != nil || job.Status != queueQueued {
		t.Fatalf("queued HTTP job = %#v, %v", job, err)
	}
}

func TestHibernatedStatusAndMetrics(t *testing.T) {
	m, _ := testPolicyManager(t, Config{})
	rt := m.get("instance-1")
	rt.mu.Lock()
	rt.paused = true
	rt.mu.Unlock()
	if got := m.statusOf(rt); got != "hibernated" {
		t.Fatalf("status=%q, want hibernated", got)
	}
	metrics := m.metricsText()
	if !strings.Contains(metrics, `whatsmeow_instances{status="hibernated"} 1`) {
		t.Fatalf("metrics missing hibernated gauge:\n%s", metrics)
	}
}

func TestGlobalSendConcurrencyHonorsContext(t *testing.T) {
	m, _ := testPolicyManager(t, Config{})
	m.sendSem = make(chan struct{}, 1)
	if err := m.acquireSendSlot(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := m.acquireSendSlot(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second slot error = %v, want deadline exceeded", err)
	}
	m.releaseSendSlot()
}
