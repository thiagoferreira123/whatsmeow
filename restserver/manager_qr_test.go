package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
)

func TestConnectEndpointTimesOutAndCancelsStalledFirstQR(t *testing.T) {
	manager, _ := testPolicyManager(t, Config{})
	runtime := manager.get("instance-1")
	manager.attachClient(runtime, &store.Device{})
	qrContext, cancel := context.WithCancel(context.Background())
	defer cancel()

	runtime.mu.Lock()
	runtime.qrRunning = true
	runtime.qrCancel = cancel
	runtime.mu.Unlock()

	request := httptest.NewRequest(http.MethodPost, "/instance/connect", nil)
	request.Header.Set("token", "token")
	response := httptest.NewRecorder()
	NewHandlers(manager, Config{}).Router().ServeHTTP(response, request)

	if response.Code != http.StatusGatewayTimeout {
		t.Errorf("POST /instance/connect status = %d; want %d", response.Code, http.StatusGatewayTimeout)
	}

	runtime.mu.RLock()
	running, activeCancel := runtime.qrRunning, runtime.qrCancel
	runtime.mu.RUnlock()
	if running || activeCancel != nil {
		t.Fatalf("stalled QR attempt remained active: running=%v cancelSet=%v", running, activeCancel != nil)
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
