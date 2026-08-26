package main

import (
	"context"
	"encoding/base64"
	"errors"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/skip2/go-qrcode"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// instanceRuntime is the in-memory state for one instance: its persisted meta,
// the live whatsmeow client, and the current QR code (if pairing).
type instanceRuntime struct {
	mu            sync.RWMutex
	meta          Instance
	client        *whatsmeow.Client
	qrCode        string
	qrExpiresAt   time.Time
	qrRunning     bool
	qrCancel      context.CancelFunc
	qrAttempt     uint64
	qrStartedAt   time.Time // início da tentativa de pareamento em voo (detecta loop travado)
	loggedOut     bool      // real unlink (needs a new QR) — watchdog skips it
	paused        bool      // intentional disconnect — watchdog must NOT reconnect
	conflicted    bool      // another live client replaced this session
	resetting     bool      // controlled runtime reset in progress
	nextConnectAt time.Time // watchdog backoff: don't attempt before this
	connectFails  int       // consecutive failed watchdog attempts (exponential backoff)
}

func (rt *instanceRuntime) metaCopy() Instance {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.meta
}

// Manager owns all instances and their whatsmeow clients.
type Manager struct {
	mu        sync.RWMutex
	runtimes  map[string]*instanceRuntime
	container *sqlstore.Container
	store     *Store
	cfg       Config
	webhooks  *WebhookSender
	outbound  *outboundGuard
	log       waLog.Logger

	// connectSem bounds simultaneous Connect() attempts (boot + watchdog) so
	// hundreds of instances don't storm WhatsApp/CPU/SQLite at the same time.
	connectSem       chan struct{}
	sendSem          chan struct{}
	queueCancel      context.CancelFunc
	queueWG          sync.WaitGroup
	logCleanupCancel context.CancelFunc
	logCleanupWG     sync.WaitGroup
	stats            managerStats

	jidMu    sync.Mutex
	jidCache map[string]jidCacheEntry // instanceID|digits -> resolved JID (TTL)

	gwMu sync.RWMutex
	gw   GlobalWebhook // single global webhook (WhatsApp Cloud API style)

	// sentEchoIDs registra os IDs de mensagens enviadas POR ESTA API para que o
	// eco fromMe correspondente seja classificado como wasSentByApi=true no
	// webhook por instância. O ID é registrado ANTES do envio (o eco pode chegar
	// antes do response). TTL curto; perda no restart só degrada para
	// wasSentByApi=false (lado seguro: bots silenciam).
	sentEchoMu  sync.Mutex
	sentEchoIDs map[string]time.Time

	// history: colheita opt-in de HistorySync para mineração de base (history.go)
	history *historyHarvester

	// Costuras do pareamento: são as DUAS únicas chamadas de I/O do fluxo de QR.
	// nil = produção (chama a lib direto); os testes substituem para não discar
	// para o WhatsApp de verdade. Escritas só na construção do Manager.
	openQRChannelFn func(context.Context, *whatsmeow.Client) (<-chan whatsmeow.QRChannelItem, error)
	dialFn          func(*whatsmeow.Client) error

	runtimeActive atomic.Bool
}

func (m *Manager) openQRChannel(ctx context.Context, cli *whatsmeow.Client) (<-chan whatsmeow.QRChannelItem, error) {
	if m.openQRChannelFn != nil {
		return m.openQRChannelFn(ctx, cli)
	}
	return cli.GetQRChannel(ctx)
}

func (m *Manager) dial(cli *whatsmeow.Client) error {
	if m.dialFn != nil {
		return m.dialFn(cli)
	}
	return cli.Connect()
}

// reviveWindow: 0 significa DESLIGADO (descarta o vínculo na hora), não default.
// Errar para o lado de gerar QR é o comportamento seguro.
func (m *Manager) reviveWindow() time.Duration {
	return time.Duration(m.cfg.QRReviveSeconds) * time.Second
}

func (m *Manager) firstCodeWait() time.Duration {
	if m.cfg.QRFirstCodeWaitSeconds <= 0 {
		return 5 * time.Second
	}
	return time.Duration(m.cfg.QRFirstCodeWaitSeconds) * time.Second
}

func (m *Manager) stallAfter() time.Duration {
	if m.cfg.QRStallSeconds <= 0 {
		return 10 * time.Second
	}
	return time.Duration(m.cfg.QRStallSeconds) * time.Second
}

const sentEchoTTL = 2 * time.Hour

// recordSentEchoID marks id as sent by this API. Prunes expired entries lazily.
func (m *Manager) recordSentEchoID(id string) {
	if id == "" {
		return
	}
	m.sentEchoMu.Lock()
	defer m.sentEchoMu.Unlock()
	if len(m.sentEchoIDs) > 4096 {
		cutoff := time.Now().Add(-sentEchoTTL)
		for k, t := range m.sentEchoIDs {
			if t.Before(cutoff) {
				delete(m.sentEchoIDs, k)
			}
		}
	}
	m.sentEchoIDs[id] = time.Now()
}

// wasSentByAPI reports whether the fromMe echo id belongs to an API-originated send.
func (m *Manager) wasSentByAPI(id string) bool {
	m.sentEchoMu.Lock()
	defer m.sentEchoMu.Unlock()
	t, ok := m.sentEchoIDs[id]
	return ok && time.Since(t) < sentEchoTTL
}

// sendRecorded sends msg with a pre-generated message ID already registered in
// sentEchoIDs, so the fromMe echo never races the classification.
func (m *Manager) sendRecorded(ctx context.Context, rt *instanceRuntime, jid types.JID, msg *waE2E.Message) (whatsmeow.SendResponse, error) {
	msgID := rt.client.GenerateMessageID()
	m.recordSentEchoID(msgID)
	return rt.client.SendMessage(ctx, jid, msg, whatsmeow.SendRequestExtra{ID: msgID})
}

// clientLogout tenta o unlink remoto tolerando runtime sem client/store — a
// lib faz deref direto em cli.Store.ID, e o caminho de auto-cura do QR pode
// chegar aqui com o device já apagado.
func (m *Manager) clientLogout(ctx context.Context, cli *whatsmeow.Client) error {
	if cli == nil || cli.Store == nil || cli.Store.ID == nil {
		return whatsmeow.ErrNotLoggedIn
	}
	return cli.Logout(ctx)
}

// Logout desvincula a sessão do WhatsApp (some de "Aparelhos conectados") e
// prepara um device NOVO para re-pareamento por QR — preservando a linha da
// instância (id, token, webhook). Se o unlink remoto falhar (ex.: sessão
// offline), o device local é descartado mesmo assim; nesse caso o pareamento
// antigo precisa ser removido manualmente no celular.
func (m *Manager) Logout(ctx context.Context, id string) (map[string]any, error) {
	rt := m.get(id)
	if rt == nil {
		return nil, errNotFound
	}
	m.invalidateQR(rt)
	rt.mu.RLock()
	cli := rt.client
	rt.mu.RUnlock()

	remote := true
	if err := m.clientLogout(ctx, cli); err != nil {
		remote = false
		m.log.Warnf("instance %s: logout remoto falhou (%v); descartando sessão local mesmo assim", id, err)
		if cli != nil {
			cli.Disconnect()
			if cli.Store != nil && cli.Store.ID != nil {
				if derr := cli.Store.Delete(ctx); derr != nil {
					m.log.Warnf("instance %s: falha ao apagar device local: %v", id, derr)
				}
			}
		}
	}
	m.attachClient(rt, m.container.NewDevice())
	rt.mu.Lock()
	rt.loggedOut = false // logout intencional para re-parear: QR liberado
	rt.paused = false
	rt.conflicted = false
	rt.meta.Status = "disconnected"
	rt.meta.JID = ""
	in := rt.meta
	rt.mu.Unlock()
	_ = m.store.Save(&in)
	m.auditInstance(id, logCategorySystem, "logout_for_repair", "warning", InstanceLog{
		Status: "disconnected", Source: "api", Details: map[string]any{"remoteLogout": remote},
	})
	return map[string]any{"instanceId": id, "loggedOut": true, "remoteLogout": remote}, nil
}

func NewManager(container *sqlstore.Container, store *Store, cfg Config, log waLog.Logger) *Manager {
	conc := cfg.ConnectConcurrency
	if conc <= 0 {
		conc = 8
	}
	sendConc := cfg.GlobalSendConcurrency
	if sendConc <= 0 {
		sendConc = 8
	}
	m := &Manager{
		runtimes:   make(map[string]*instanceRuntime),
		container:  container,
		store:      store,
		cfg:        cfg,
		webhooks:   NewWebhookSender(),
		outbound:   newOutboundGuard(cfg),
		log:        log,
		connectSem:  make(chan struct{}, conc),
		sendSem:     make(chan struct{}, sendConc),
		jidCache:    make(map[string]jidCacheEntry),
		sentEchoIDs: make(map[string]time.Time),
		history:     newHistoryHarvester(),
	}
	m.webhooks.onFailure = func(url string, out deliveryOutcome) {
		log.Warnf("webhook delivery failed after %d attempts (status=%d err=%v) url=%s",
			out.Attempts, out.StatusCode, out.Err, url)
	}
	m.loadGlobalWebhook()
	m.runtimeActive.Store(true)
	return m
}

func (m *Manager) SetRuntimeActive(active bool) { m.runtimeActive.Store(active) }

func (m *Manager) RuntimeActive() bool { return m.runtimeActive.Load() }

// connectWithLimit runs cli.Connect() holding a slot of the global connect
// semaphore. Call from a goroutine; blocking here only delays other connects.
func (m *Manager) connectWithLimit(rt *instanceRuntime, cli *whatsmeow.Client, reason string) {
	m.connectSem <- struct{}{}
	defer func() { <-m.connectSem }()
	if cli.IsConnected() {
		return
	}
	instanceID := rt.metaCopy().ID
	m.auditInstance(instanceID, logCategoryConnection, "connect_attempt", "info", InstanceLog{
		Status: "connecting", Source: reason,
	})
	m.stats.connectAttempts.Add(1)
	if err := cli.Connect(); err != nil && !errors.Is(err, whatsmeow.ErrAlreadyConnected) {
		m.stats.connectFailures.Add(1)
		rt.mu.Lock()
		rt.resetting = false
		rt.mu.Unlock()
		m.log.Warnf("%s: connect %s failed: %v", reason, rt.meta.ID, err)
		m.auditInstance(instanceID, logCategoryConnection, "connect_attempt_failed", "error", InstanceLog{
			Status: "disconnected", Source: reason, Reason: err.Error(),
		})
	} else {
		m.log.Infof("%s: connecting %s", reason, rt.meta.ID)
		m.auditInstance(instanceID, logCategoryConnection, "connect_started", "info", InstanceLog{
			Status: "connecting", Source: reason,
		})
	}
}

func (m *Manager) get(id string) *instanceRuntime {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.runtimes[id]
}

// invalidateQR mata a tentativa de pareamento em voo: invalida o consumidor
// (qrAttempt), zera o código e cancela o contexto do canal de QR. O cancel roda
// FORA do lock — ele chega até o socket da lib.
func (m *Manager) invalidateQR(rt *instanceRuntime) {
	rt.mu.Lock()
	cancel := rt.qrCancel
	rt.qrAttempt++
	rt.qrRunning = false
	rt.qrCode = ""
	rt.qrExpiresAt = time.Time{}
	rt.qrStartedAt = time.Time{}
	rt.qrCancel = nil
	rt.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// attachClient é o ÚNICO ponto de troca de client de uma instância. Além de
// instalar o novo, ele descarta por completo o anterior: sem isso o client
// velho seguia conectado despachando eventos no MESMO handler da instância, e o
// loop de QR órfão fazia todo pedido seguinte esperar por um código que nunca
// chegaria.
func (m *Manager) attachClient(rt *instanceRuntime, device *store.Device) {
	cli := whatsmeow.NewClient(device, m.log)
	cli.EnableAutoReconnect = true // recover from socket drops without a new QR (default true)
	// Só faz sentido para device pareado: com Store.ID nil o autoReconnect da lib
	// é no-op (client.go:606-609), então a flag apenas engoliria o erro de dial do
	// pareamento e transformaria a falha em espera muda por um QR.
	cli.InitialAutoReconnect = device != nil && device.ID != nil
	cli.AddEventHandler(m.makeHandler(rt.metaCopy().ID))

	m.invalidateQR(rt)
	rt.mu.Lock()
	previous := rt.client
	rt.client = cli
	rt.mu.Unlock()
	if previous != nil && previous != cli {
		go previous.Disconnect() // pega o socketLock da lib: nunca sob rt.mu
	}
}

// StartWatchdog periodically re-Connects paired instances that are down. It's a
// safety net on top of whatsmeow's own auto-reconnect (covers boot-time connect
// failures and conflict drops). Connect() is serialized by the client's
// socketLock, so calling it here is safe even if a reconnect is already running.
func (m *Manager) StartWatchdog(interval time.Duration) {
	if interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			m.reconnectStale()
		}
	}()
}

// reconnectBackoff returns how long to wait before the next attempt after
// `fails` consecutive failures: 30s, 1m, 2m, … capped at 10m, with ±20% jitter
// so a fleet that went down together doesn't retry in lockstep.
func reconnectBackoff(fails int) time.Duration {
	base := 30 * time.Second
	for i := 0; i < fails && base < 10*time.Minute; i++ {
		base *= 2
	}
	if base > 10*time.Minute {
		base = 10 * time.Minute
	}
	jitter := 0.8 + 0.4*rand.Float64()
	return time.Duration(float64(base) * jitter)
}

func (m *Manager) reconnectStale() {
	m.mu.RLock()
	rts := make([]*instanceRuntime, 0, len(m.runtimes))
	for _, rt := range m.runtimes {
		rts = append(rts, rt)
	}
	m.mu.RUnlock()

	now := time.Now()
	for _, rt := range rts {
		rt.mu.RLock()
		cli := rt.client
		skip := rt.loggedOut || rt.paused || rt.conflicted || rt.qrRunning || now.Before(rt.nextConnectAt)
		rt.mu.RUnlock()
		if cli == nil || skip {
			continue
		}
		if cli.Store == nil || cli.Store.ID == nil { // never paired -> needs QR
			continue
		}
		if cli.IsConnected() {
			continue
		}
		rt.mu.Lock()
		rt.nextConnectAt = now.Add(reconnectBackoff(rt.connectFails))
		rt.connectFails++ // reset to 0 by onConnected
		rt.mu.Unlock()
		go m.connectWithLimit(rt, cli, "watchdog")
	}
}

// LoadAll rehydrates instances from the DB on boot and reconnects paired ones.
func (m *Manager) LoadAll(ctx context.Context) error {
	list, err := m.store.List()
	if err != nil {
		return err
	}
	for _, in := range list {
		rt := &instanceRuntime{
			meta:       in,
			paused:     in.Status == "hibernated",
			conflicted: strings.HasPrefix(in.LastDisconnectReason, "stream_replaced"),
		}
		var device *store.Device
		if in.JID != "" {
			if jid, perr := types.ParseJID(in.JID); perr == nil {
				device, _ = m.container.GetDevice(ctx, jid)
			}
		}
		if device == nil {
			device = m.container.NewDevice()
		}
		m.attachClient(rt, device)

		m.mu.Lock()
		m.runtimes[in.ID] = rt
		m.mu.Unlock()

		if device.ID != nil && !rt.paused && !rt.conflicted {
			go m.connectWithLimit(rt, rt.client, "boot")
		}
	}
	return nil
}

// Shutdown cleanly disconnects every client (proper websocket close) so
// sessions resume instantly on the next boot. Bounded by the caller's patience.
func (m *Manager) Shutdown() {
	if m.queueCancel != nil {
		m.queueCancel()
	}
	if m.logCleanupCancel != nil {
		m.logCleanupCancel()
	}
	m.mu.RLock()
	rts := make([]*instanceRuntime, 0, len(m.runtimes))
	for _, rt := range m.runtimes {
		rts = append(rts, rt)
	}
	m.mu.RUnlock()

	var wg sync.WaitGroup
	for _, rt := range rts {
		rt.mu.RLock()
		cli := rt.client
		rt.mu.RUnlock()
		if cli == nil || !cli.IsConnected() {
			continue
		}
		wg.Add(1)
		go func(cli *whatsmeow.Client) {
			defer wg.Done()
			cli.Disconnect()
		}(cli)
	}
	wg.Wait()
	m.queueWG.Wait()
	m.logCleanupWG.Wait()
}

const (
	defaultUazapiWebhookEvents          = "connection,messages"
	defaultUazapiWebhookExcludeMessages = "wasSentByApi,fromMeYes,isGroupYes"
)

// Create registers a new instance (no pairing yet — call GetQR to pair).
func (m *Manager) Create(name, adminField01, webhookURL, webhookSecret string) (Instance, error) {
	now := nowRFC()
	in := Instance{
		ID:                     uuid.NewString(),
		Name:                   name,
		Token:                  randToken(),
		AdminField01:           adminField01,
		WebhookURL:             webhookURL,
		WebhookSecret:          webhookSecret,
		WebhookEvents:          defaultUazapiWebhookEvents,
		WebhookExcludeMessages: defaultUazapiWebhookExcludeMessages,
		WebhookEnabled:         true,
		Status:                 "disconnected",
		CreatedAt:              now,
		UpdatedAt:              now,
	}
	if err := m.store.Create(&in); err != nil {
		return Instance{}, err
	}
	rt := &instanceRuntime{meta: in}
	m.attachClient(rt, m.container.NewDevice())
	m.mu.Lock()
	m.runtimes[in.ID] = rt
	m.mu.Unlock()
	m.auditInstance(in.ID, logCategorySystem, "instance_created", "info", InstanceLog{
		Status: in.Status, Source: "api", Details: map[string]any{"name": in.Name},
	})
	return in, nil
}

func (m *Manager) List() ([]Instance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Instance, 0, len(m.runtimes))
	for _, rt := range m.runtimes {
		in := rt.metaCopy()
		in.Status = m.statusOf(rt)
		out = append(out, in)
	}
	return out, nil
}

func (m *Manager) Get(id string) (Instance, error) {
	rt := m.get(id)
	if rt == nil {
		return Instance{}, errNotFound
	}
	in := rt.metaCopy()
	in.Status = m.statusOf(rt)
	return in, nil
}

func (m *Manager) statusOf(rt *instanceRuntime) string {
	rt.mu.RLock()
	cli := rt.client
	qr := rt.qrCode
	running := rt.qrRunning
	paused := rt.paused
	rt.mu.RUnlock()
	if cli != nil && cli.IsConnected() && cli.IsLoggedIn() {
		return "connected"
	}
	if running || qr != "" {
		return "connecting"
	}
	if paused {
		return "hibernated"
	}
	return "disconnected"
}

// StatusDetail returns a live status object for the status endpoint.
func (m *Manager) StatusDetail(id string) (map[string]any, error) {
	rt := m.get(id)
	if rt == nil {
		return nil, errNotFound
	}
	in := rt.metaCopy()
	rt.mu.RLock()
	cli := rt.client
	paused, conflicted, resetting := rt.paused, rt.conflicted, rt.resetting
	currentQR := rt.qrCode
	rt.mu.RUnlock()
	loggedIn := cli != nil && cli.IsLoggedIn()
	owner := in.Owner
	if cli != nil && cli.Store != nil && cli.Store.ID != nil {
		owner = cli.Store.ID.User
	}
	result := map[string]any{
		"id":                  id,
		"status":              m.statusOf(rt),
		"loggedIn":            loggedIn,
		"connected":           cli != nil && cli.IsConnected(),
		"owner":               owner,
		"profileName":         in.ProfileName,
		"profilePicUrl":       in.ProfilePicUrl,
		"isBusiness":          in.IsBusiness,
		"sendingBlockedUntil": in.SendingBlockedUntil,
		"lastResetAt":         in.LastResetAt,
		"hibernated":          paused,
		"conflicted":          conflicted,
		"resetting":           resetting,
	}
	if currentQR != "" {
		if png, err := qrcode.Encode(currentQR, qrcode.Medium, 256); err == nil {
			result["qrcode"] = "data:image/png;base64," + base64.StdEncoding.EncodeToString(png)
		}
	}
	if queue, err := m.store.QueueSummary(id); err == nil {
		result["queue"] = queue
	}
	return result, nil
}

// QR returns the current QR (as a PNG data URI) and raw code, starting the
// pairing flow if needed. If already paired/connected it reports the status.
func (m *Manager) QR(ctx context.Context, id string) (map[string]any, error) {
	code, expires, status, err := m.qrCode(ctx, id)
	if err != nil {
		return nil, err
	}
	res := map[string]any{"status": status}
	if code != "" {
		png, perr := qrcode.Encode(code, qrcode.Medium, 256)
		if perr == nil {
			res["qrcode"] = "data:image/png;base64," + base64.StdEncoding.EncodeToString(png)
		}
		res["code"] = code
		res["expiresAt"] = expires.UTC().Format(time.RFC3339)
	}
	return res, nil
}

// QRPNG returns the raw PNG bytes of the current QR code (for browser preview).
func (m *Manager) QRPNG(ctx context.Context, id string) ([]byte, error) {
	code, _, _, err := m.qrCode(ctx, id)
	if err != nil {
		return nil, err
	}
	if code == "" {
		return nil, nil
	}
	return qrcode.Encode(code, qrcode.Medium, 512)
}

var (
	// errPairingInFlight: outra requisição ganhou a corrida da reserva.
	errPairingInFlight = errors.New("pairing already in flight")
	// errPairingSuperseded: a tentativa foi invalidada no meio (troca de client,
	// disconnect, delete). Re-planejar é o certo.
	errPairingSuperseded = errors.New("pairing attempt superseded")
)

// qrCode garante que um pedido de QR termine em código sempre que a instância
// NÃO estiver conectada e logada — hibernada, conflitada, com device apagado,
// com socket zumbi ou com um loop de pareamento travado inclusive.
//
// Cada volta aplica UMA ação de cura (planQRRequest) e reduz estritamente o
// espaço de estados, então o laço converge bem antes de qrMaxPlanSteps.
func (m *Manager) qrCode(ctx context.Context, id string) (code string, expires time.Time, status string, err error) {
	rt := m.get(id)
	if rt == nil {
		return "", time.Time{}, "", errNotFound
	}
	brakesReleased := false
	for step := 0; step < qrMaxPlanSteps; step++ {
		snap, cli := m.snapshotQR(rt)
		action := planQRRequest(snap)
		if action == qrReportConnected {
			return "", time.Time{}, "connected", nil
		}
		// Pedir QR é uma ordem explícita de trazer a instância de volta: nenhum
		// freio (hibernação, conflito, backoff) sobrevive a isso.
		if !brakesReleased {
			m.releaseBrakes(rt, id)
			brakesReleased = true
		}

		switch action {
		case qrRecycleDevice:
			if !m.recycleDevice(rt, id, "device inutilizável para parear") {
				return "", time.Time{}, "", m.pairingUnavailable(id, errors.New("device store indisponível"))
			}

		case qrReviveSession:
			if m.reviveSession(ctx, rt, cli, id) {
				return "", time.Time{}, "connected", nil
			}
			if m.cfg.QRKeepSessionOnReviveFailure {
				return "", time.Time{}, "connecting", nil // kill-switch: preserva o vínculo
			}
			m.discardLink(ctx, id)

		case qrDropSocket:
			if cli != nil {
				cli.Disconnect() // GetQRChannel exige socket fechado
			}

		case qrRestartPairing:
			m.invalidateQR(rt)

		case qrServeCurrent, qrStartPairing:
			if action == qrStartPairing {
				perr := m.startPairing(rt, cli, id)
				switch {
				case perr == nil, errors.Is(perr, errPairingInFlight):
					// segue para a espera do primeiro código
				case errors.Is(perr, errPairingSuperseded):
					continue
				case m.recoverFromPairErr(rt, cli, id, perr):
					continue
				default:
					return "", time.Time{}, "", m.pairingUnavailable(id, perr)
				}
			}
			c, exp, outcome := m.awaitFirstQR(ctx, rt)
			switch outcome {
			case qrAwaitGot:
				return c, exp, "connecting", nil
			case qrAwaitSuperseded:
				continue
			default:
				m.invalidateQR(rt)
				m.auditInstance(id, logCategoryConnection, "pairing_first_qr_timeout", "warning", InstanceLog{
					Status: "disconnected", Source: "qr",
					Reason: "primeiro código não chegou na janela; tentativa descartada",
				})
				return "", time.Time{}, "", &apiError{
					Status:     http.StatusServiceUnavailable,
					RetryAfter: 2,
					Msg:        "não foi possível gerar o QR agora; tente novamente",
				}
			}
		}
	}
	return "", time.Time{}, "", m.pairingUnavailable(id, errors.New("estado do pareamento não convergiu"))
}

// snapshotQR fotografa o estado para a decisão. Os campos do runtime saem sob
// RLock; os métodos do client são chamados FORA dele (pegam o socketLock da lib,
// e o caminho inverso — evento → handler → rt.mu — fecharia um deadlock).
func (m *Manager) snapshotQR(rt *instanceRuntime) (qrSnapshot, *whatsmeow.Client) {
	rt.mu.RLock()
	cli := rt.client
	snap := qrSnapshot{
		qrRunning:  rt.qrRunning,
		hasCode:    rt.qrCode != "",
		stallAfter: m.stallAfter(),
	}
	if !rt.qrStartedAt.IsZero() {
		snap.qrAge = time.Since(rt.qrStartedAt)
	}
	rt.mu.RUnlock()

	switch {
	case cli == nil:
		snap.clientNil = true
	case cli.Store == nil:
		snap.storeNil = true
	default:
		snap.deviceDeleted = cli.Store.Deleted
		snap.hasSession = cli.Store.ID != nil
		snap.connected = cli.IsConnected()
		snap.loggedIn = cli.IsLoggedIn()
	}
	return snap, cli
}

// releaseBrakes solta tudo que impediria a instância de voltar: hibernação,
// conflito e backoff do watchdog. Persiste quando o meta muda, porque LoadAll
// reconstrói `paused` de Status=="hibernated" e `conflicted` do prefixo
// "stream_replaced" — sem gravar, o bloqueio voltaria no próximo boot.
func (m *Manager) releaseBrakes(rt *instanceRuntime, id string) {
	rt.mu.Lock()
	rt.paused = false
	rt.conflicted = false
	rt.nextConnectAt = time.Time{}
	rt.connectFails = 0
	metaChanged := false
	if rt.meta.Status == "hibernated" {
		rt.meta.Status = "connecting"
		metaChanged = true
	}
	if strings.HasPrefix(rt.meta.LastDisconnectReason, "stream_replaced") {
		rt.meta.LastDisconnectReason = ""
		metaChanged = true
	}
	in := rt.meta
	rt.mu.Unlock()
	if metaChanged {
		if err := m.store.Save(&in); err != nil {
			m.log.Warnf("instance %s: falha ao persistir a liberação dos freios: %v", id, err)
		}
	}
}

// recycleDevice troca por um device virgem. É a única saída para um *Client com
// Store.Deleted: a lib não tem "undelete" e o Connect falha para sempre.
func (m *Manager) recycleDevice(rt *instanceRuntime, id, reason string) bool {
	if m.container == nil {
		return false
	}
	m.attachClient(rt, m.container.NewDevice())
	m.auditInstance(id, logCategoryConnection, "device_recycled", "warning", InstanceLog{
		Status: "disconnected", Source: "qr", Reason: reason,
	})
	return true
}

// reviveSession tenta trazer de volta uma sessão salva que só está offline,
// para não obrigar o profissional a reparear por uma queda passageira.
func (m *Manager) reviveSession(ctx context.Context, rt *instanceRuntime, cli *whatsmeow.Client, id string) bool {
	window := m.reviveWindow()
	if window <= 0 || cli == nil {
		return false
	}
	if !cli.IsConnected() {
		if err := m.dial(cli); err != nil {
			m.log.Warnf("instance %s: revive falhou no connect: %v", id, err)
		}
	}
	recovered := waitReconnect(ctx, window, 250*time.Millisecond, func() bool {
		return cli.IsConnected() && cli.IsLoggedIn()
	})
	m.auditInstance(id, logCategoryConnection, "qr_revive_attempt", "info", InstanceLog{
		Status: "connecting", Source: "qr",
		Details: map[string]any{"recovered": recovered, "windowSeconds": int(window.Seconds())},
	})
	return recovered
}

// discardLink desvincula a sessão que não voltou, liberando um QR novo. Reusa o
// Logout: unlink remoto best-effort + device novo + flags limpas + persistência.
func (m *Manager) discardLink(ctx context.Context, id string) {
	lctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := m.Logout(lctx, id); err != nil {
		m.log.Warnf("instance %s: falha ao descartar o vínculo antes do QR: %v", id, err)
		return
	}
	m.auditInstance(id, logCategoryConnection, "qr_forced_relink", "warning", InstanceLog{
		Status: "disconnected", Source: "qr",
		Reason: "sessão salva não voltou na janela de revive; novo QR necessário",
	})
}

// startPairing reserva a tentativa (a reserva é a exclusão mútua entre
// requisições concorrentes), abre o canal de QR e disca — nessa ordem, porque
// GetQRChannel tem que vir ANTES do Connect (qrchan.go:191-196).
func (m *Manager) startPairing(rt *instanceRuntime, cli *whatsmeow.Client, id string) error {
	if cli == nil {
		return whatsmeow.ErrClientIsNil
	}
	rt.mu.Lock()
	if rt.qrRunning {
		rt.mu.Unlock()
		return errPairingInFlight
	}
	rt.qrAttempt++
	attempt := rt.qrAttempt
	rt.qrRunning = true
	rt.qrStartedAt = time.Now()
	rt.qrCode = ""
	rt.qrExpiresAt = time.Time{}
	harvest := m.history != nil && m.history.enabledFor(rt.meta)
	rt.mu.Unlock()

	// Full history sync SÓ para instâncias na lista de colheita — pareamentos
	// normais (nutricionist_*) não são afetados. DeviceProps é global da lib,
	// então o valor vale para o pareamento iniciado agora; corrida entre dois
	// pareamentos simultâneos é aceitável (raro e o efeito é só sync mais lento).
	store.DeviceProps.RequireFullSync = proto.Bool(harvest)
	if harvest {
		m.log.Infof("instance %s: pareamento com RequireFullSync=true (colheita de histórico)", id)
	}

	qrCtx, cancel := context.WithCancel(context.Background())
	qrChan, qerr := m.openQRChannel(qrCtx, cli)
	if qerr != nil {
		cancel()
		m.releasePairing(rt, attempt)
		return qerr
	}
	if cerr := m.dial(cli); cerr != nil && !errors.Is(cerr, whatsmeow.ErrAlreadyConnected) {
		cancel()
		m.releasePairing(rt, attempt)
		return cerr
	}
	rt.mu.Lock()
	if rt.qrAttempt != attempt {
		rt.mu.Unlock()
		cancel()
		return errPairingSuperseded
	}
	rt.qrCancel = cancel
	rt.mu.Unlock()
	m.auditInstance(id, logCategoryConnection, "pairing_started", "info", InstanceLog{
		Status: "connecting", Source: "qr",
	})
	go m.consumeQR(rt, attempt, qrChan)
	return nil
}

// releasePairing devolve a reserva quando o pareamento não chegou a começar.
func (m *Manager) releasePairing(rt *instanceRuntime, attempt uint64) {
	rt.mu.Lock()
	if rt.qrAttempt == attempt {
		rt.qrRunning = false
		rt.qrStartedAt = time.Time{}
	}
	rt.mu.Unlock()
}

// recoverFromPairErr aplica a cura do erro e diz se vale re-planejar.
func (m *Manager) recoverFromPairErr(rt *instanceRuntime, cli *whatsmeow.Client, id string, err error) bool {
	switch {
	case errors.Is(err, whatsmeow.ErrClientIsNil), errors.Is(err, store.ErrDeviceDeleted):
		return m.recycleDevice(rt, id, err.Error())
	case errors.Is(err, whatsmeow.ErrQRAlreadyConnected):
		if cli != nil {
			cli.Disconnect()
		}
		return true
	case errors.Is(err, whatsmeow.ErrQRStoreContainsID):
		// Pareou entre o snapshot e a chamada: re-planejar leva a revive (ou a
		// "connected", se o login concluiu).
		return true
	}
	return false
}

type qrAwaitOutcome uint8

const (
	qrAwaitGot qrAwaitOutcome = iota
	qrAwaitSuperseded
	qrAwaitTimedOut
)

// awaitFirstQR espera o primeiro código da tentativa corrente.
func (m *Manager) awaitFirstQR(ctx context.Context, rt *instanceRuntime) (string, time.Time, qrAwaitOutcome) {
	wait := m.firstCodeWait()
	poll := 200 * time.Millisecond
	if adaptive := wait / 10; adaptive > 0 && adaptive < poll {
		poll = adaptive
	}
	rt.mu.RLock()
	attempt := rt.qrAttempt
	rt.mu.RUnlock()

	deadline := time.Now().Add(wait)
	for {
		rt.mu.RLock()
		code, exp := rt.qrCode, rt.qrExpiresAt
		current, running := rt.qrAttempt, rt.qrRunning
		rt.mu.RUnlock()
		if current != attempt {
			return "", time.Time{}, qrAwaitSuperseded
		}
		if code != "" {
			return code, exp, qrAwaitGot
		}
		if !running {
			return "", time.Time{}, qrAwaitSuperseded
		}
		if !time.Now().Before(deadline) {
			return "", time.Time{}, qrAwaitTimedOut
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", time.Time{}, qrAwaitTimedOut
		case <-timer.C:
		}
	}
}

// pairingUnavailable é a única saída de erro do fluxo de QR além do 404: sempre
// retryable e auditada, nunca 500 cru nem 504.
func (m *Manager) pairingUnavailable(id string, cause error) error {
	reason := ""
	if cause != nil {
		reason = cause.Error()
	}
	m.auditInstance(id, logCategoryConnection, "pairing_unavailable", "error", InstanceLog{
		Status: "disconnected", Source: "qr", Reason: reason,
	})
	return &apiError{
		Status:     http.StatusServiceUnavailable,
		RetryAfter: 3,
		Msg:        "não foi possível gerar o QR agora; tente novamente em instantes",
	}
}

func (m *Manager) consumeQR(rt *instanceRuntime, attempt uint64, ch <-chan whatsmeow.QRChannelItem) {
	for evt := range ch {
		rt.mu.RLock()
		current := rt.qrAttempt == attempt
		rt.mu.RUnlock()
		if !current {
			continue
		}
		if evt.Event == whatsmeow.QRChannelEventCode {
			rt.mu.Lock()
			if rt.qrAttempt != attempt {
				rt.mu.Unlock()
				continue
			}
			rt.qrCode = evt.Code
			rt.qrExpiresAt = time.Now().Add(evt.Timeout)
			rt.mu.Unlock()
			m.auditInstance(rt.metaCopy().ID, logCategoryConnection, "qr_generated", "info", InstanceLog{
				Status: "connecting", Source: "qr", Details: map[string]any{"expiresInSeconds": int(evt.Timeout.Seconds())},
			})
		} else { // success / timeout / error
			rt.mu.Lock()
			if rt.qrAttempt == attempt {
				rt.qrCode = ""
				rt.qrExpiresAt = time.Time{}
			}
			rt.mu.Unlock()
		}
	}
	rt.mu.Lock()
	if rt.qrAttempt == attempt {
		rt.qrRunning = false
		rt.qrCode = ""
		rt.qrExpiresAt = time.Time{}
		rt.qrCancel = nil
	}
	rt.mu.Unlock()
}

// Disconnect closes the socket but keeps the session (reconnect without re-scan).
func (m *Manager) Disconnect(id string) error {
	rt := m.get(id)
	if rt == nil {
		return errNotFound
	}
	m.invalidateQR(rt)
	rt.mu.Lock()
	rt.meta.Status = "hibernated"
	rt.paused = true // intentional — the watchdog must leave it down
	in := rt.meta
	cli := rt.client
	rt.mu.Unlock()
	if cli != nil {
		cli.Disconnect()
	}
	if err := m.store.Save(&in); err != nil {
		return err
	}
	m.auditInstance(id, logCategoryConnection, "hibernated", "warning", InstanceLog{
		Status: "hibernated", Source: "operator", Reason: "socket disconnected intentionally",
	})
	return nil
}

// Resume brings back a hibernated or conflict-paused session without a new QR.
func (m *Manager) Resume(id string) error {
	rt := m.get(id)
	if rt == nil {
		return errNotFound
	}
	cli := rt.client
	if cli == nil || cli.Store == nil || cli.Store.ID == nil {
		return &apiError{Status: 409, Msg: "instance has no persisted session; pair with QR first"}
	}
	rt.mu.Lock()
	rt.paused = false
	rt.conflicted = false
	rt.meta.Status = "connecting"
	rt.meta.LastDisconnectReason = ""
	rt.nextConnectAt = time.Time{}
	in := rt.meta
	rt.mu.Unlock()
	if err := m.store.Save(&in); err != nil {
		return err
	}
	m.auditInstance(id, logCategoryConnection, "resume_requested", "info", InstanceLog{
		Status: "connecting", Source: "operator",
	})
	go m.connectWithLimit(rt, cli, "resume")
	return nil
}

// ResetRuntime performs a controlled socket restart without deleting session
// credentials. It is cooldown-protected and also recovers stuck queue jobs.
func (m *Manager) ResetRuntime(id string) (map[string]any, error) {
	rt := m.get(id)
	if rt == nil {
		return nil, errNotFound
	}
	cli := rt.client
	rt.mu.RLock()
	loggedOut := rt.loggedOut
	rt.mu.RUnlock()
	if cli == nil || cli.Store == nil || cli.Store.ID == nil || loggedOut {
		return nil, &apiError{Status: 409, Msg: "session is not recoverable; a new QR is required"}
	}
	now := time.Now()
	rt.mu.Lock()
	lastReset := parseStoredTime(rt.meta.LastResetAt)
	cooldown := time.Duration(m.cfg.ResetCooldownSeconds) * time.Second
	if rt.resetting {
		rt.mu.Unlock()
		return map[string]any{"instanceId": id, "resetting": true, "queuedRecoveryAttempted": false}, nil
	}
	if !lastReset.IsZero() && cooldown > 0 && now.Sub(lastReset) < cooldown {
		wait := cooldown - now.Sub(lastReset)
		rt.mu.Unlock()
		return nil, rateLimitError("runtime reset cooldown is active", wait)
	}
	rt.resetting = true
	rt.paused = false
	rt.conflicted = false
	rt.nextConnectAt = time.Time{}
	rt.meta.Status = "connecting"
	rt.meta.LastDisconnectReason = ""
	rt.meta.LastResetAt = now.UTC().Format(time.RFC3339)
	in := rt.meta
	rt.mu.Unlock()
	if err := m.store.Save(&in); err != nil {
		rt.mu.Lock()
		rt.resetting = false
		rt.mu.Unlock()
		return nil, err
	}
	m.invalidateQR(rt)
	cli.Disconnect()
	recovered, qerr := m.store.RecoverInstanceQueue(id)
	m.stats.resets.Add(1)
	m.auditInstance(id, logCategorySystem, "runtime_reset", "warning", InstanceLog{
		Status: "connecting", Source: "operator", Details: map[string]any{"queuedRecovered": recovered, "queueRecoveryError": qerr != nil},
	})
	go func() {
		time.Sleep(500 * time.Millisecond)
		m.connectWithLimit(rt, cli, "runtime reset")
	}()
	return map[string]any{
		"instanceId":              id,
		"resetting":               true,
		"queuedRecoveryAttempted": qerr == nil,
		"queuedRecovered":         recovered,
	}, nil
}

// Delete logs out (if paired), removes the device store, the runtime, and the row.
func (m *Manager) Delete(ctx context.Context, id string) error {
	rt := m.get(id)
	if rt == nil {
		return errNotFound
	}
	m.auditInstance(id, logCategorySystem, "instance_delete_requested", "warning", InstanceLog{Source: "operator"})
	m.invalidateQR(rt)
	rt.mu.RLock()
	cli := rt.client
	rt.mu.RUnlock()
	if cli != nil {
		if cli.IsLoggedIn() {
			_ = cli.Logout(ctx) // logs out of WhatsApp and deletes the device store
		} else {
			cli.Disconnect()
			if cli.Store != nil && cli.Store.ID != nil {
				_ = m.container.DeleteDevice(ctx, cli.Store)
			}
		}
	}
	m.mu.Lock()
	delete(m.runtimes, id)
	m.mu.Unlock()
	return m.store.Delete(id)
}

// SetWebhook updates the per-instance webhook config.
func (m *Manager) SetWebhook(id, url, secret, events string, enabled bool) error {
	rt := m.get(id)
	if rt == nil {
		return errNotFound
	}
	rt.mu.Lock()
	rt.meta.WebhookURL = url
	if secret != "" {
		rt.meta.WebhookSecret = secret
	}
	if events != "" {
		rt.meta.WebhookEvents = events
	}
	rt.meta.WebhookEnabled = enabled
	in := rt.meta
	rt.mu.Unlock()
	return m.store.Save(&in)
}

// SetUazapiWebhook updates every field supported by the uazapi simple-mode
// webhook contract while keeping the instance secret server-controlled.
func (m *Manager) SetUazapiWebhook(id string, cfg uazapiWebhookConfig) error {
	rt := m.get(id)
	if rt == nil {
		return errNotFound
	}
	rt.mu.Lock()
	rt.meta.WebhookURL = cfg.URL
	rt.meta.WebhookEvents = strings.Join(cfg.Events, ",")
	rt.meta.WebhookExcludeMessages = strings.Join(cfg.ExcludeMessages, ",")
	rt.meta.WebhookEnabled = cfg.Enabled
	rt.meta.WebhookAddURLEvents = cfg.AddURLEvents
	rt.meta.WebhookAddURLTypesMessages = cfg.AddURLTypesMessages
	in := rt.meta
	rt.mu.Unlock()
	return m.store.Save(&in)
}

// jidCacheEntry is a resolved recipient JID with an expiry.
type jidCacheEntry struct {
	jid types.JID
	exp time.Time
}

const (
	jidCacheTTL     = 12 * time.Hour
	jidCacheMaxSize = 50_000
)

func (m *Manager) cachedJID(key string) (types.JID, bool) {
	m.jidMu.Lock()
	defer m.jidMu.Unlock()
	e, ok := m.jidCache[key]
	if !ok || time.Now().After(e.exp) {
		return types.JID{}, false
	}
	return e.jid, true
}

func (m *Manager) storeJID(key string, jid types.JID) {
	m.jidMu.Lock()
	defer m.jidMu.Unlock()
	if len(m.jidCache) >= jidCacheMaxSize {
		now := time.Now()
		for k, e := range m.jidCache {
			if now.After(e.exp) {
				delete(m.jidCache, k)
			}
		}
		if len(m.jidCache) >= jidCacheMaxSize { // still full of live entries: drop all (rare)
			m.jidCache = make(map[string]jidCacheEntry)
		}
	}
	m.jidCache[key] = jidCacheEntry{jid: jid, exp: time.Now().Add(jidCacheTTL)}
}

// resolveRecipient resolves a phone number to its canonical WhatsApp JID by
// asking the server (IsOnWhatsApp), trying the Brazilian 9th-digit variants.
// Successful lookups are cached per instance for jidCacheTTL so repeat sends
// skip the network round-trip (and its rate-limit exposure).
// A value already containing "@" is parsed as a JID and returned as-is.
func (m *Manager) resolveRecipient(ctx context.Context, cli *whatsmeow.Client, number string) (types.JID, error) {
	n := strings.TrimSpace(number)
	if n == "" {
		return types.JID{}, &apiError{Status: 400, Msg: "número é obrigatório"}
	}
	if strings.Contains(n, "@") {
		jid, err := types.ParseJID(n)
		if err != nil {
			return types.JID{}, &apiError{Status: 400, Msg: "JID inválido: " + n}
		}
		return jid, nil
	}
	digits := nonDigit.ReplaceAllString(n, "")
	if digits == "" {
		return types.JID{}, &apiError{Status: 400, Msg: "número inválido"}
	}
	cacheKey := digits
	if cli.Store != nil && cli.Store.ID != nil {
		cacheKey = cli.Store.ID.User + "|" + digits
	}
	if jid, ok := m.cachedJID(cacheKey); ok {
		return jid, nil
	}
	candidates := phoneCandidates(digits)

	resp, err := cli.IsOnWhatsApp(ctx, withPlus(candidates))
	if err != nil {
		// Lookup failed (e.g. transient): fall back to the number as typed.
		m.log.Warnf("IsOnWhatsApp lookup failed for %s, using as-typed: %v", digits, err)
		return types.NewJID(digits, types.DefaultUserServer), nil
	}
	for _, r := range resp {
		if r.IsIn {
			m.storeJID(cacheKey, r.JID)
			return r.JID, nil
		}
	}
	return types.JID{}, &apiError{Status: 422, Msg: "número não está no WhatsApp: " + digits}
}

// SendText sends a plain text message and returns the message id.
func (m *Manager) SendText(ctx context.Context, id, number, text string) (messageID string, sendErr error) {
	audit := sendAuditFromContext(ctx)
	attemptedRecipient := permissionKey(number)
	resolvedRecipient := ""
	m.auditSendAttempt(id, attemptedRecipient, "text", audit)
	defer func() {
		m.auditSendResult(id, attemptedRecipient, resolvedRecipient, "text", messageID, sendErr, audit)
	}()
	rt, err := m.requireLoggedIn(id)
	if err != nil {
		return "", err
	}
	if err := m.acquireSendSlot(ctx); err != nil {
		return "", err
	}
	defer m.releaseSendSlot()
	jid, err := m.resolveRecipient(ctx, rt.client, number)
	if err != nil {
		return "", err
	}
	recipient := permissionKey(jid.User)
	resolvedRecipient = recipient
	m.rememberRecipientAlias(id, attemptedRecipient, jid)
	if err := m.checkOutbound(ctx, id, recipient); err != nil {
		return "", err
	}
	resp, err := m.sendRecorded(ctx, rt, jid, &waE2E.Message{Conversation: proto.String(text)})
	if err != nil {
		m.stats.sendFailures.Add(1)
		return "", err
	}
	m.stats.sendSuccess.Add(1)
	if err := m.store.RecordOutbound(id, recipient, time.Now()); err != nil {
		m.log.Warnf("failed to audit outbound message %s: %v", resp.ID, err)
	}
	m.log.Infof("sent text to %s (msg %s)", jid, resp.ID)
	return resp.ID, nil
}

// sendTextJID sends a reply to an already-known chat while applying the same
// outbound policy as REST-initiated messages.
func (m *Manager) sendTextJID(ctx context.Context, id string, jid types.JID, text string) (messageID string, sendErr error) {
	audit := sendAuditFromContext(ctx)
	attemptedRecipient := permissionKey(jid.User)
	m.auditSendAttempt(id, attemptedRecipient, "text", audit)
	defer func() {
		m.auditSendResult(id, attemptedRecipient, attemptedRecipient, "text", messageID, sendErr, audit)
	}()
	rt, err := m.requireLoggedIn(id)
	if err != nil {
		return "", err
	}
	recipient := permissionKey(jid.User)
	if err := m.checkOutbound(ctx, id, recipient); err != nil {
		return "", err
	}
	if err := m.acquireSendSlot(ctx); err != nil {
		return "", err
	}
	defer m.releaseSendSlot()
	resp, err := m.sendRecorded(ctx, rt, jid, &waE2E.Message{Conversation: proto.String(text)})
	if err != nil {
		m.stats.sendFailures.Add(1)
		return "", err
	}
	m.stats.sendSuccess.Add(1)
	if err := m.store.RecordOutbound(id, recipient, time.Now()); err != nil {
		m.log.Warnf("failed to audit outbound message %s: %v", resp.ID, err)
	}
	return resp.ID, nil
}

// SendMedia uploads and sends an image/video/audio/document message.
func (m *Manager) SendMedia(ctx context.Context, id, number, mediaType, file, caption, fileName string) (messageID string, sendErr error) {
	audit := sendAuditFromContext(ctx)
	if mediaType == "" {
		mediaType = "media"
	}
	attemptedRecipient := permissionKey(number)
	resolvedRecipient := ""
	m.auditSendAttempt(id, attemptedRecipient, mediaType, audit)
	defer func() {
		m.auditSendResult(id, attemptedRecipient, resolvedRecipient, mediaType, messageID, sendErr, audit)
	}()
	rt, err := m.requireLoggedIn(id)
	if err != nil {
		return "", err
	}
	if err := m.acquireSendSlot(ctx); err != nil {
		return "", err
	}
	defer m.releaseSendSlot()
	jid, err := m.resolveRecipient(ctx, rt.client, number)
	if err != nil {
		return "", err
	}
	recipient := permissionKey(jid.User)
	resolvedRecipient = recipient
	if err := m.checkOutbound(ctx, id, recipient); err != nil {
		return "", err
	}
	msg, err := buildMediaMessage(ctx, rt.client, mediaType, file, caption, fileName)
	if err != nil {
		m.stats.sendFailures.Add(1)
		return "", err
	}
	resp, err := m.sendRecorded(ctx, rt, jid, msg)
	if err != nil {
		m.stats.sendFailures.Add(1)
		return "", err
	}
	m.stats.sendSuccess.Add(1)
	if err := m.store.RecordOutbound(id, recipient, time.Now()); err != nil {
		m.log.Warnf("failed to audit outbound message %s: %v", resp.ID, err)
	}
	m.log.Infof("sent %s to %s (msg %s)", mediaType, jid, resp.ID)
	return resp.ID, nil
}

// SendMediaBytes sends an uploaded file (raw bytes) as media.
func (m *Manager) SendMediaBytes(ctx context.Context, id, number, mediaType string, data []byte, mime, caption, fileName string) (messageID string, sendErr error) {
	audit := sendAuditFromContext(ctx)
	if mediaType == "" {
		mediaType = "media"
	}
	attemptedRecipient := permissionKey(number)
	resolvedRecipient := ""
	m.auditSendAttempt(id, attemptedRecipient, mediaType, audit)
	defer func() {
		m.auditSendResult(id, attemptedRecipient, resolvedRecipient, mediaType, messageID, sendErr, audit)
	}()
	rt, err := m.requireLoggedIn(id)
	if err != nil {
		return "", err
	}
	if err := m.acquireSendSlot(ctx); err != nil {
		return "", err
	}
	defer m.releaseSendSlot()
	jid, err := m.resolveRecipient(ctx, rt.client, number)
	if err != nil {
		return "", err
	}
	recipient := permissionKey(jid.User)
	resolvedRecipient = recipient
	if err := m.checkOutbound(ctx, id, recipient); err != nil {
		return "", err
	}
	msg, err := buildMediaMessageBytes(ctx, rt.client, mediaType, data, mime, caption, fileName)
	if err != nil {
		m.stats.sendFailures.Add(1)
		return "", err
	}
	resp, err := m.sendRecorded(ctx, rt, jid, msg)
	if err != nil {
		m.stats.sendFailures.Add(1)
		return "", err
	}
	m.stats.sendSuccess.Add(1)
	if err := m.store.RecordOutbound(id, recipient, time.Now()); err != nil {
		m.log.Warnf("failed to audit outbound message %s: %v", resp.ID, err)
	}
	m.log.Infof("sent uploaded %s to %s (msg %s)", mediaType, jid, resp.ID)
	return resp.ID, nil
}

var errNotConnected = &apiError{Status: 409, Msg: "instance is not connected/logged in"}

func (m *Manager) acquireSendSlot(ctx context.Context) error {
	select {
	case m.sendSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) releaseSendSlot() { <-m.sendSem }

func (m *Manager) requireLoggedIn(id string) (*instanceRuntime, error) {
	rt := m.get(id)
	if rt == nil {
		return nil, errNotFound
	}
	if rt.client == nil || !rt.client.IsLoggedIn() {
		return nil, errNotConnected
	}
	return rt, nil
}
