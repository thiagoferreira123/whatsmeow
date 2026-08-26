package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// qrTestConfig: revive desligado (0 = descarta na hora) e esperas curtas, para a
// suíte não arrastar. Nenhum teste deste arquivo toca a rede.
func qrTestConfig() Config {
	return Config{QRReviveSeconds: 0, QRFirstCodeWaitSeconds: 1, QRStallSeconds: 10}
}

// pairingStub substitui as duas únicas chamadas de I/O do pareamento
// (GetQRChannel e Connect) para que os testes não disquem para o WhatsApp.
type pairingStub struct {
	opens    atomic.Int64
	dials    atomic.Int64
	openErrs []error
	dialErr  error
	code     string
	started  chan struct{}
	release  chan struct{}

	mu    sync.Mutex
	chans []chan whatsmeow.QRChannelItem
}

func stubPairing(t *testing.T, m *Manager, code string, openErrs ...error) *pairingStub {
	t.Helper()
	s := &pairingStub{code: code, openErrs: openErrs, started: make(chan struct{}, 16)}
	m.openQRChannelFn = func(ctx context.Context, cli *whatsmeow.Client) (<-chan whatsmeow.QRChannelItem, error) {
		n := int(s.opens.Add(1)) - 1
		select {
		case s.started <- struct{}{}:
		default:
		}
		if s.release != nil {
			<-s.release
		}
		if n < len(s.openErrs) && s.openErrs[n] != nil {
			return nil, s.openErrs[n]
		}
		ch := make(chan whatsmeow.QRChannelItem, 4)
		ch <- whatsmeow.QRChannelItem{Event: whatsmeow.QRChannelEventCode, Code: s.code, Timeout: time.Minute}
		s.mu.Lock()
		s.chans = append(s.chans, ch)
		s.mu.Unlock()
		return ch, nil
	}
	m.dialFn = func(cli *whatsmeow.Client) error {
		s.dials.Add(1)
		return s.dialErr
	}
	t.Cleanup(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, ch := range s.chans {
			close(ch)
		}
	})
	return s
}

func pairedDevice(t *testing.T, m *Manager, user string) *store.Device {
	t.Helper()
	device := m.container.NewDevice()
	jid := types.NewJID(user, types.DefaultUserServer)
	device.ID = &jid
	return device
}

func requireQR(t *testing.T, code string, status string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("qrCode devolveu erro %v (status=%q)", err, status)
	}
	if code == "" {
		t.Fatalf("nenhum QR gerado (status=%q)", status)
	}
	if status != "connecting" {
		t.Fatalf("status = %q; quero \"connecting\"", status)
	}
}

func instanceRow(t *testing.T, m *Manager, id string) Instance {
	t.Helper()
	in, err := m.store.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	return in
}

func auditEvents(t *testing.T, m *Manager, id string) map[string]bool {
	t.Helper()
	logs, err := m.store.ListInstanceLogs(id, InstanceLogQuery{Limit: 200})
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, l := range logs {
		found[l.Event] = true
	}
	return found
}

// 401/403/406: a lib apaga o device e o *Client passa a recusar todo Connect().
func TestQRAfterLoggedOutGeneratesNewCode(t *testing.T) {
	ctx := context.Background()
	m, rt := testManagerWithDeviceStore(t, qrTestConfig())
	stub := stubPairing(t, m, "codigo-pos-logout")
	device := pairedDevice(t, m, "5531920023640")
	m.attachClient(rt, device)
	if err := device.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	m.onLoggedOut("instance-1", &events.LoggedOut{OnConnect: true, Reason: events.ConnectFailureLoggedOut})

	code, _, status, err := m.qrCode(ctx, "instance-1")
	requireQR(t, code, status, err)
	if code != stub.code {
		t.Fatalf("code = %q; quero %q", code, stub.code)
	}
}

// PairError tardio apaga o device SEM emitir LoggedOut: o pedido de QR tem que
// se curar sozinho mesmo que nenhum handler tenha reciclado antes.
func TestQRRecyclesDeletedDeviceWithoutLoggedOutEvent(t *testing.T) {
	ctx := context.Background()
	m, rt := testManagerWithDeviceStore(t, qrTestConfig())
	stubPairing(t, m, "codigo-device-reciclado")
	device := pairedDevice(t, m, "5531920023640")
	m.attachClient(rt, device)
	if err := device.Delete(ctx); err != nil {
		t.Fatal(err)
	}

	code, _, status, err := m.qrCode(ctx, "instance-1")
	requireQR(t, code, status, err)

	rt.mu.RLock()
	cli := rt.client
	rt.mu.RUnlock()
	if cli.Store.Deleted {
		t.Fatal("o device apagado continuou anexado")
	}
}

func TestQRForHibernatedInstanceGeneratesCode(t *testing.T) {
	ctx := context.Background()
	m, rt := testManagerWithDeviceStore(t, qrTestConfig())
	stubPairing(t, m, "codigo-hibernada")
	m.attachClient(rt, m.container.NewDevice())
	rt.mu.Lock()
	rt.paused = true
	rt.meta.Status = "hibernated"
	in := rt.meta
	rt.mu.Unlock()
	if err := m.store.Save(&in); err != nil {
		t.Fatal(err)
	}

	code, _, status, err := m.qrCode(ctx, "instance-1")
	requireQR(t, code, status, err)

	rt.mu.RLock()
	paused := rt.paused
	rt.mu.RUnlock()
	if paused {
		t.Fatal("a instância continuou pausada — o watchdog seguiria ignorando")
	}
	if got := instanceRow(t, m, "instance-1").Status; got == "hibernated" {
		t.Fatal("o banco ficou com status=hibernated: LoadAll re-hibernaria no próximo boot")
	}
}

func TestQRForConflictedInstanceGeneratesCode(t *testing.T) {
	ctx := context.Background()
	m, rt := testManagerWithDeviceStore(t, qrTestConfig())
	stubPairing(t, m, "codigo-conflitada")
	m.attachClient(rt, m.container.NewDevice())
	rt.mu.Lock()
	rt.conflicted = true
	rt.nextConnectAt = time.Now().Add(5 * time.Minute)
	rt.meta.LastDisconnectReason = "stream_replaced (mesma sessão conectou em outro lugar)"
	in := rt.meta
	rt.mu.Unlock()
	if err := m.store.Save(&in); err != nil {
		t.Fatal(err)
	}

	code, _, status, err := m.qrCode(ctx, "instance-1")
	requireQR(t, code, status, err)

	rt.mu.RLock()
	conflicted := rt.conflicted
	rt.mu.RUnlock()
	if conflicted {
		t.Fatal("a flag de conflito sobreviveu ao pedido de QR")
	}
	if reason := instanceRow(t, m, "instance-1").LastDisconnectReason; strings.HasPrefix(reason, "stream_replaced") {
		t.Fatalf("LastDisconnectReason=%q faria LoadAll marcar conflito de novo", reason)
	}
}

// Caso central da decisão do produto: sessão salva que não volta na janela de
// revive é descartada para dar lugar a um QR novo.
func TestQRDiscardsSavedSessionWhenReviveWindowExpires(t *testing.T) {
	ctx := context.Background()
	m, rt := testManagerWithDeviceStore(t, qrTestConfig())
	stubPairing(t, m, "codigo-pos-descarte")
	m.attachClient(rt, pairedDevice(t, m, "5531920023640"))
	rt.mu.Lock()
	rt.meta.JID = "5531920023640@s.whatsapp.net"
	in := rt.meta
	rt.mu.Unlock()
	if err := m.store.Save(&in); err != nil {
		t.Fatal(err)
	}

	code, _, status, err := m.qrCode(ctx, "instance-1")
	requireQR(t, code, status, err)

	rt.mu.RLock()
	cli := rt.client
	rt.mu.RUnlock()
	if cli.Store.ID != nil {
		t.Fatalf("o vínculo antigo não foi descartado: ID=%v", cli.Store.ID)
	}
	if cli.Store.Deleted {
		t.Fatal("o device anexado está apagado — o pareamento falharia")
	}
	if row := instanceRow(t, m, "instance-1"); row.JID != "" {
		t.Fatalf("JID=%q deveria ter sido limpo junto com o vínculo", row.JID)
	}
	events := auditEvents(t, m, "instance-1")
	if !events["logout_for_repair"] || !events["qr_forced_relink"] {
		t.Fatalf("o descarte do vínculo não foi auditado: %v", events)
	}
}

func TestQRNeverReportsConnectedForOfflinePairedInstance(t *testing.T) {
	ctx := context.Background()
	m, rt := testManagerWithDeviceStore(t, qrTestConfig())
	stubPairing(t, m, "codigo-offline")
	m.attachClient(rt, pairedDevice(t, m, "5531920023640"))

	code, _, status, err := m.qrCode(ctx, "instance-1")
	if err != nil {
		t.Fatal(err)
	}
	if status == "connected" {
		t.Fatal("instância offline reportada como conectada")
	}
	if code == "" {
		t.Fatal("instância offline ficou sem QR")
	}
}

// Antes: loop órfão fazia todo pedido esperar 5s e devolver 504 para sempre.
func TestQRRestartsStalledPairingLoopInsteadOfTimingOut(t *testing.T) {
	ctx := context.Background()
	m, rt := testManagerWithDeviceStore(t, qrTestConfig())
	stub := stubPairing(t, m, "codigo-pos-travamento")
	m.attachClient(rt, m.container.NewDevice())

	stalledCtx, cancel := context.WithCancel(context.Background())
	rt.mu.Lock()
	rt.qrRunning = true
	rt.qrStartedAt = time.Now().Add(-time.Minute)
	rt.qrCancel = cancel
	before := rt.qrAttempt
	rt.mu.Unlock()

	code, _, status, err := m.qrCode(ctx, "instance-1")
	requireQR(t, code, status, err)
	if stub.opens.Load() != 1 {
		t.Fatalf("abriu %d canais de QR; quero 1", stub.opens.Load())
	}
	rt.mu.RLock()
	after := rt.qrAttempt
	rt.mu.RUnlock()
	if after == before {
		t.Fatal("o loop travado não foi invalidado")
	}
	select {
	case <-stalledCtx.Done():
	default:
		t.Fatal("o contexto do loop travado não foi cancelado")
	}
}

func TestQRRecoversFromOpenChannelErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"socket de pé sem login", whatsmeow.ErrQRAlreadyConnected},
		{"device pareado entre o snapshot e a chamada", whatsmeow.ErrQRStoreContainsID},
		{"client nulo", whatsmeow.ErrClientIsNil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			m, rt := testManagerWithDeviceStore(t, qrTestConfig())
			stub := stubPairing(t, m, "codigo-pos-recuperacao", tc.err)
			m.attachClient(rt, m.container.NewDevice())

			code, _, status, err := m.qrCode(ctx, "instance-1")
			requireQR(t, code, status, err)
			if stub.opens.Load() < 2 {
				t.Fatalf("não houve nova tentativa depois de %v", tc.err)
			}
		})
	}
}

// Quando é fisicamente impossível gerar código, o erro tem que ser retryable —
// nunca 500 cru nem 504.
func TestQRSurfacesDialFailureAsRetryableAPIError(t *testing.T) {
	ctx := context.Background()
	m, rt := testManagerWithDeviceStore(t, qrTestConfig())
	stub := stubPairing(t, m, "codigo-que-nao-sai")
	stub.dialErr = errors.New("dial tcp: connection refused")
	m.attachClient(rt, m.container.NewDevice())

	_, _, _, err := m.qrCode(ctx, "instance-1")
	if err == nil {
		t.Fatal("falha de dial deveria virar erro")
	}
	if got := apiStatus(err); got != http.StatusServiceUnavailable {
		t.Fatalf("status = %d; quero %d (%v)", got, http.StatusServiceUnavailable, err)
	}
	var ae *apiError
	if errors.As(err, &ae) && ae.RetryAfter <= 0 {
		t.Fatal("o erro precisa trazer Retry-After para o cliente tentar de novo")
	}
	rt.mu.RLock()
	running := rt.qrRunning
	rt.mu.RUnlock()
	if running {
		t.Fatal("a reserva do pareamento ficou presa depois da falha")
	}
}

// O lock do runtime era segurado através de GetQRChannel + Connect, travando
// status, disconnect e os handlers de evento durante todo o dial.
func TestQRDoesNotHoldRuntimeLockDuringPairing(t *testing.T) {
	m, rt := testManagerWithDeviceStore(t, qrTestConfig())
	stub := stubPairing(t, m, "codigo-sem-lock")
	stub.release = make(chan struct{})
	m.attachClient(rt, m.container.NewDevice())

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _, _ = m.qrCode(context.Background(), "instance-1")
	}()
	<-stub.started

	read := make(chan struct{})
	go func() {
		defer close(read)
		_ = m.statusOf(rt)
		_, _ = m.StatusDetail("instance-1")
	}()
	select {
	case <-read:
	case <-time.After(2 * time.Second):
		t.Fatal("statusOf/StatusDetail travaram durante o pareamento")
	}
	close(stub.release)
	<-done
}

func TestConcurrentQRRequestsStartASinglePairingLoop(t *testing.T) {
	m, rt := testManagerWithDeviceStore(t, qrTestConfig())
	stub := stubPairing(t, m, "codigo-concorrente")
	m.attachClient(rt, m.container.NewDevice())

	const callers = 10
	var wg sync.WaitGroup
	codes := make([]string, callers)
	errs := make([]error, callers)
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func(i int) {
			defer wg.Done()
			codes[i], _, _, errs[i] = m.qrCode(context.Background(), "instance-1")
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("chamada %d falhou: %v", i, err)
		}
		if codes[i] != stub.code {
			t.Fatalf("chamada %d devolveu %q; quero %q", i, codes[i], stub.code)
		}
	}
	if got := stub.dials.Load(); got != 1 {
		t.Fatalf("%d dials; quero exatamente 1", got)
	}
}

// O objeto era montado ANTES de gerar o QR, então a resposta saía com QR e
// `hibernated: true` ao mesmo tempo — estado que já não existe mais.
func TestUzConnectForHibernatedReportsFreshState(t *testing.T) {
	m := testUazapiCompatManager(t, qrTestConfig())
	stubPairing(t, m, "codigo-http-hibernada")
	in, err := m.Create("nutricionist_2", "2", "", "")
	if err != nil {
		t.Fatal(err)
	}
	rt := m.get(in.ID)
	m.attachClient(rt, m.container.NewDevice())
	if err := m.Disconnect(in.ID); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/instance/connect", nil)
	req.Header.Set("token", in.Token)
	rec := httptest.NewRecorder()
	NewHandlers(m, qrTestConfig()).Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("POST /instance/connect = %d; body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Instance struct {
			Status     string `json:"status"`
			QRCode     string `json:"qrcode"`
			Hibernated bool   `json:"hibernated"`
		} `json:"instance"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Instance.QRCode == "" {
		t.Fatal("instância hibernada respondeu sem QR")
	}
	if body.Instance.Hibernated {
		t.Fatal("resposta traz QR e hibernated=true ao mesmo tempo")
	}
	if body.Instance.Status != "connecting" {
		t.Fatalf("status = %q; quero \"connecting\"", body.Instance.Status)
	}
}

func TestUzConnectReturnsQRForDeletedDevice(t *testing.T) {
	ctx := context.Background()
	m := testUazapiCompatManager(t, qrTestConfig())
	stubPairing(t, m, "codigo-http")
	in, err := m.Create("nutricionist_1", "1", "", "")
	if err != nil {
		t.Fatal(err)
	}
	rt := m.get(in.ID)
	device := pairedDevice(t, m, "5531920023640")
	m.attachClient(rt, device)
	if err := device.Delete(ctx); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/instance/connect", nil)
	req.Header.Set("token", in.Token)
	rec := httptest.NewRecorder()
	NewHandlers(m, qrTestConfig()).Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("POST /instance/connect = %d; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "data:image/png;base64,") {
		t.Fatalf("resposta sem QR: %s", rec.Body.String())
	}
}
