package main

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// noBackoff keeps the retry tests instantaneous. The production backoff
// (1s, 2s) is exercised implicitly by the default constructor.
func testSender() *WebhookSender {
	ws := NewWebhookSender()
	ws.retryBackoff = func(int) time.Duration { return 0 }
	return ws
}

func serverReturning(t *testing.T, codes ...int) (*httptest.Server, *int32) {
	t.Helper()
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(atomic.AddInt32(&hits, 1))
		code := codes[len(codes)-1]
		if n <= len(codes) {
			code = codes[n-1]
		}
		w.WriteHeader(code)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func TestDeliverSyncSucceedsOnFirstAttempt(t *testing.T) {
	srv, hits := serverReturning(t, http.StatusOK)

	out := testSender().deliverSync(srv.URL, "secret", []byte(`{}`))

	if !out.Delivered {
		t.Fatalf("expected delivered, got %+v", out)
	}
	if out.Attempts != 1 || atomic.LoadInt32(hits) != 1 {
		t.Fatalf("expected exactly 1 attempt, got attempts=%d hits=%d", out.Attempts, *hits)
	}
}

// REGRESSAO: um 401 significava "entregue" e sumia sem rastro. Foi exatamente
// esse silencio que escondeu por 5 semanas o gate de secret quebrado no
// DietSystem: a uazapi respondia 401 a cada "1"/"2" de paciente e nada era
// registrado em lugar nenhum.
func TestDeliverSyncTreats4xxAsFailureWithoutRetrying(t *testing.T) {
	srv, hits := serverReturning(t, http.StatusUnauthorized)

	out := testSender().deliverSync(srv.URL, "secret", []byte(`{}`))

	if out.Delivered {
		t.Fatal("401 must NOT count as delivered")
	}
	if out.Attempts != 1 || atomic.LoadInt32(hits) != 1 {
		t.Fatalf("config error must not be retried, got attempts=%d hits=%d", out.Attempts, *hits)
	}
	if out.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected status 401 surfaced, got %d", out.StatusCode)
	}
}

func TestDeliverSyncRetriesServerErrors(t *testing.T) {
	srv, hits := serverReturning(t, http.StatusInternalServerError)

	out := testSender().deliverSync(srv.URL, "secret", []byte(`{}`))

	if out.Delivered {
		t.Fatal("expected failure after exhausting retries")
	}
	if out.Attempts != webhookMaxAttempts || int(atomic.LoadInt32(hits)) != webhookMaxAttempts {
		t.Fatalf("expected %d attempts, got attempts=%d hits=%d", webhookMaxAttempts, out.Attempts, *hits)
	}
}

func TestDeliverSyncRecoversAfterTransientServerError(t *testing.T) {
	srv, hits := serverReturning(t, http.StatusBadGateway, http.StatusOK)

	out := testSender().deliverSync(srv.URL, "secret", []byte(`{}`))

	if !out.Delivered {
		t.Fatalf("expected delivery on the retry, got %+v", out)
	}
	if out.Attempts != 2 || atomic.LoadInt32(hits) != 2 {
		t.Fatalf("expected 2 attempts, got attempts=%d hits=%d", out.Attempts, *hits)
	}
}

// 429/408 sao transitorios (rate limit / timeout), nao erro de configuracao:
// precisam de retry como os 5xx.
func TestDeliverSyncRetriesRateLimitAndTimeout(t *testing.T) {
	for _, code := range []int{http.StatusTooManyRequests, http.StatusRequestTimeout} {
		srv, hits := serverReturning(t, code, http.StatusOK)

		out := testSender().deliverSync(srv.URL, "secret", []byte(`{}`))

		if !out.Delivered || out.Attempts != 2 {
			t.Fatalf("status %d must be retried, got %+v (hits=%d)", code, out, *hits)
		}
	}
}

func TestDeliverSyncSendsSecretHeader(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("x-uazapi-secret")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	testSender().deliverSync(srv.URL, "s3cr3t", []byte(`{}`))

	if got != "s3cr3t" {
		t.Fatalf("expected the uazapi-compat secret header, got %q", got)
	}
}

func TestDeliverSyncReportsTransportFailure(t *testing.T) {
	ws := testSender()

	// Porta fechada: erro de transporte, nao HTTP status.
	out := ws.deliverSync("http://127.0.0.1:1/webhook", "secret", []byte(`{}`))

	if out.Delivered {
		t.Fatal("expected failure when the endpoint is unreachable")
	}
	if out.Err == nil {
		t.Fatal("expected the transport error to be surfaced")
	}
	if out.Attempts != webhookMaxAttempts {
		t.Fatalf("expected %d attempts, got %d", webhookMaxAttempts, out.Attempts)
	}
}

// Uma entrega perdida precisa deixar rastro — esse e o ponto central da
// mudanca. Sem isso, "o paciente respondeu 1 e nao aconteceu nada" nao tem
// nenhuma evidencia para investigar.
func TestDeliverLogsFailure(t *testing.T) {
	srv, _ := serverReturning(t, http.StatusUnauthorized)

	var failures int32
	var lastStatus int32
	ws := testSender()
	ws.onFailure = func(url string, out deliveryOutcome) {
		atomic.AddInt32(&failures, 1)
		atomic.StoreInt32(&lastStatus, int32(out.StatusCode))
	}

	ws.deliver(srv.URL, "secret", map[string]any{"EventType": "messages"})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && atomic.LoadInt32(&failures) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt32(&failures) != 1 {
		t.Fatal("a dropped webhook delivery must be reported, not silently swallowed")
	}
	if atomic.LoadInt32(&lastStatus) != http.StatusUnauthorized {
		t.Fatalf("expected the 401 to be reported, got %d", lastStatus)
	}
}

func TestDeliverDoesNotReportSuccess(t *testing.T) {
	srv, _ := serverReturning(t, http.StatusNoContent)

	var failures int32
	ws := testSender()
	ws.onFailure = func(string, deliveryOutcome) { atomic.AddInt32(&failures, 1) }

	ws.deliver(srv.URL, "secret", map[string]any{"EventType": "messages"})

	time.Sleep(300 * time.Millisecond)
	if atomic.LoadInt32(&failures) != 0 {
		t.Fatal("a successful delivery must not be reported as a failure")
	}
}

func TestDeliverIgnoresEmptyURL(t *testing.T) {
	var failures int32
	ws := testSender()
	ws.onFailure = func(string, deliveryOutcome) { atomic.AddInt32(&failures, 1) }

	ws.deliver("", "secret", map[string]any{"EventType": "messages"})

	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt32(&failures) != 0 {
		t.Fatal("an instance without a webhook URL is not a delivery failure")
	}
}
