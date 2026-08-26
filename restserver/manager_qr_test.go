package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
)

// Um loop de pareamento travado deixava TODO pedido seguinte esperar 5s e sair
// 504 para sempre. Agora ele é descartado e o mesmo pedido já devolve QR novo.
func TestConnectEndpointRestartsStalledFirstQRInsteadOfTimingOut(t *testing.T) {
	cfg := qrTestConfig()
	manager := testUazapiCompatManager(t, cfg)
	stubPairing(t, manager, "codigo-http-pos-travamento")
	instance, err := manager.Create("nutricionist_1", "1", "", "")
	if err != nil {
		t.Fatal(err)
	}
	runtime := manager.get(instance.ID)
	manager.attachClient(runtime, manager.container.NewDevice())

	qrContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime.mu.Lock()
	runtime.qrRunning = true
	runtime.qrStartedAt = time.Now().Add(-time.Minute)
	runtime.qrCancel = cancel
	runtime.mu.Unlock()

	request := httptest.NewRequest(http.MethodPost, "/instance/connect", nil)
	request.Header.Set("token", instance.Token)
	response := httptest.NewRecorder()
	NewHandlers(manager, cfg).Router().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("POST /instance/connect status = %d; want %d (body=%s)", response.Code, http.StatusOK, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "data:image/png;base64,") {
		t.Fatalf("resposta sem QR: %s", response.Body.String())
	}

	select {
	case <-qrContext.Done():
	default:
		t.Fatal("stalled QR attempt context was not cancelled")
	}
}

func TestStaleQRConsumerDoesNotClearNewAttempt(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := &instanceRuntime{
		qrAttempt:   2,
		qrRunning:   true,
		qrCode:      "new-attempt-code",
		qrExpiresAt: time.Now().Add(time.Minute),
		qrCancel:    cancel,
	}
	staleEvents := make(chan whatsmeow.QRChannelItem, 1)
	staleEvents <- whatsmeow.QRChannelItem{
		Event:   whatsmeow.QRChannelEventCode,
		Code:    "stale-attempt-code",
		Timeout: time.Minute,
	}
	close(staleEvents)

	(&Manager{}).consumeQR(runtime, 1, staleEvents)

	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	if !runtime.qrRunning || runtime.qrCode != "new-attempt-code" || runtime.qrCancel == nil {
		t.Fatalf(
			"stale consumer changed current attempt: running=%v code=%q cancelSet=%v",
			runtime.qrRunning,
			runtime.qrCode,
			runtime.qrCancel != nil,
		)
	}
}
