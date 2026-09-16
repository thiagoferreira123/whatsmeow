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

// O WhatsApp passou a usar passkey no pareamento: a lib entrega
// QRChannelItem com Event "passkey-request"/"passkey-confirmation" no MEIO da
// tentativa, sem terminá-la — o código na tela continua válido e o usuário
// ainda precisa vê-lo. O consumidor tratava tudo que não é "code" como
// terminal e apagava o QR, deixando a tela vazia durante o pareamento.
func TestConsumeQRKeepsCodeOnPasskeyEvents(t *testing.T) {
	for _, event := range []string{"passkey-request", "passkey-confirmation"} {
		t.Run(event, func(t *testing.T) {
			runtime := &instanceRuntime{
				qrAttempt:   1,
				qrRunning:   true,
				qrCode:      "codigo-valido-na-tela",
				qrExpiresAt: time.Now().Add(time.Minute),
			}
			// Sem buffer: o segundo envio só retorna quando o consumidor volta
			// ao range, ou seja, depois de processar o primeiro. É isso que
			// torna a asserção determinística, sem sleep.
			events := make(chan whatsmeow.QRChannelItem)
			done := make(chan struct{})
			go func() {
				defer close(done)
				(&Manager{}).consumeQR(runtime, 1, events)
			}()

			events <- whatsmeow.QRChannelItem{Event: event}
			events <- whatsmeow.QRChannelItem{Event: event}

			runtime.mu.RLock()
			code, expires := runtime.qrCode, runtime.qrExpiresAt
			runtime.mu.RUnlock()

			close(events)
			<-done

			if code != "codigo-valido-na-tela" {
				t.Fatalf("evento %q apagou o QR em voo: code=%q; want %q", event, code, "codigo-valido-na-tela")
			}
			if expires.IsZero() {
				t.Fatalf("evento %q zerou a validade do QR em voo", event)
			}
		})
	}
}

// O evento terminal do canal de QR era descartado em silêncio: a auditoria
// mostrava pairing_started + N qr_generated e parava, sem dizer POR QUE o
// pareamento não fechou. Foi exatamente isso que escondeu a quebra de 15/09
// (companion_reg_refresh) por dois dias.
func TestConsumeQRAuditsTerminalEvent(t *testing.T) {
	manager := testUazapiCompatManager(t, qrTestConfig())
	instance, err := manager.Create("nutricionist_1", "1", "", "")
	if err != nil {
		t.Fatal(err)
	}
	runtime := manager.get(instance.ID)
	// Create já passou por attachClient/invalidateQR, então a tentativa corrente
	// não é zero — consumir com outro número faria o consumidor ignorar tudo.
	runtime.mu.RLock()
	attempt := runtime.qrAttempt
	runtime.mu.RUnlock()

	qrEvents := make(chan whatsmeow.QRChannelItem, 1)
	qrEvents <- whatsmeow.QRChannelTimeout
	close(qrEvents)
	manager.consumeQR(runtime, attempt, qrEvents)

	logs, err := manager.store.ListInstanceLogs(instance.ID, InstanceLogQuery{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range logs {
		if entry.Event == "pairing_ended" {
			if got := entry.Details["event"]; got != whatsmeow.QRChannelTimeout.Event {
				t.Fatalf("pairing_ended registrou event=%v; want %q", got, whatsmeow.QRChannelTimeout.Event)
			}
			return
		}
	}
	t.Fatalf("pareamento terminou sem auditar o motivo; eventos gravados: %v", auditedEvents(logs))
}

func auditedEvents(logs []InstanceLog) []string {
	names := make([]string, 0, len(logs))
	for _, entry := range logs {
		names = append(names, entry.Event)
	}
	return names
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
