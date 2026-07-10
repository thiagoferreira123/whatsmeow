package main

import (
	"context"
	"encoding/base64"
	"errors"
	"math/rand/v2"
	"strings"
	"sync"
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
		connectSem: make(chan struct{}, conc),
		sendSem:    make(chan struct{}, sendConc),
		jidCache:   make(map[string]jidCacheEntry),
	}
	m.loadGlobalWebhook()
	return m
}

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

func (m *Manager) attachClient(rt *instanceRuntime, device *store.Device) {
	cli := whatsmeow.NewClient(device, m.log)
	cli.EnableAutoReconnect = true  // recover from socket drops without a new QR (default true)
	cli.InitialAutoReconnect = true // also retry in background if the FIRST connect fails (default false)
	cli.AddEventHandler(m.makeHandler(rt.meta.ID))
	rt.mu.Lock()
	rt.client = cli
	rt.mu.Unlock()
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
	if queue, err := m.store.QueueSummary(id); err == nil {
		result["queue"] = queue
	}
	return result, nil
}

// QR returns the current QR (as a PNG data URI) and raw code, starting the
// pairing flow if needed. If already paired/connected it reports the status.
func (m *Manager) QR(id string) (map[string]any, error) {
	code, expires, status, err := m.qrCode(id)
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
func (m *Manager) QRPNG(id string) ([]byte, error) {
	code, _, _, err := m.qrCode(id)
	if err != nil {
		return nil, err
	}
	if code == "" {
		return nil, nil
	}
	return qrcode.Encode(code, qrcode.Medium, 512)
}

// qrCode ensures the pairing loop is running and returns the latest code.
func (m *Manager) qrCode(id string) (code string, expires time.Time, status string, err error) {
	rt := m.get(id)
	if rt == nil {
		return "", time.Time{}, "", errNotFound
	}
	cli := rt.client
	if cli.IsConnected() && cli.IsLoggedIn() {
		return "", time.Time{}, "connected", nil
	}
	// Already registered (has identity) but offline: just reconnect, no QR.
	if cli.Store.ID != nil {
		rt.mu.Lock()
		rt.paused = false // user asked to bring it back — re-enable the watchdog
		rt.conflicted = false
		rt.nextConnectAt = time.Time{}
		rt.mu.Unlock()
		if !cli.IsConnected() {
			go func() { _ = cli.Connect() }()
		}
		return "", time.Time{}, "connecting", nil
	}

	rt.mu.Lock()
	if !rt.qrRunning {
		qrCtx, cancel := context.WithCancel(context.Background())
		qrChan, qerr := cli.GetQRChannel(qrCtx)
		if qerr != nil {
			cancel()
			rt.mu.Unlock()
			go func() { _ = cli.Connect() }()
			return "", time.Time{}, "connecting", nil
		}
		if cerr := cli.Connect(); cerr != nil {
			cancel()
			rt.mu.Unlock()
			return "", time.Time{}, "", cerr
		}
		rt.qrRunning = true
		rt.qrCancel = cancel
		m.auditInstance(id, logCategoryConnection, "pairing_started", "info", InstanceLog{
			Status: "connecting", Source: "qr",
		})
		go m.consumeQR(rt, qrChan)
	}
	rt.mu.Unlock()

	// Wait briefly for the first code to arrive.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rt.mu.RLock()
		c, exp := rt.qrCode, rt.qrExpiresAt
		rt.mu.RUnlock()
		if c != "" {
			return c, exp, "connecting", nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return "", time.Time{}, "connecting", nil
}

func (m *Manager) consumeQR(rt *instanceRuntime, ch <-chan whatsmeow.QRChannelItem) {
	for evt := range ch {
		if evt.Event == whatsmeow.QRChannelEventCode {
			rt.mu.Lock()
			rt.qrCode = evt.Code
			rt.qrExpiresAt = time.Now().Add(evt.Timeout)
			rt.mu.Unlock()
			m.auditInstance(rt.metaCopy().ID, logCategoryConnection, "qr_generated", "info", InstanceLog{
				Status: "connecting", Source: "qr", Details: map[string]any{"expiresInSeconds": int(evt.Timeout.Seconds())},
			})
		} else { // success / timeout / error
			rt.mu.Lock()
			rt.qrCode = ""
			rt.mu.Unlock()
		}
	}
	rt.mu.Lock()
	rt.qrRunning = false
	rt.qrCode = ""
	rt.qrCancel = nil
	rt.mu.Unlock()
}

// Disconnect closes the socket but keeps the session (reconnect without re-scan).
func (m *Manager) Disconnect(id string) error {
	rt := m.get(id)
	if rt == nil {
		return errNotFound
	}
	rt.mu.Lock()
	if rt.qrCancel != nil {
		rt.qrCancel()
		rt.qrCancel = nil
	}
	rt.meta.Status = "hibernated"
	rt.paused = true // intentional — the watchdog must leave it down
	in := rt.meta
	rt.mu.Unlock()
	rt.client.Disconnect()
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
	if rt.qrCancel != nil {
		rt.qrCancel()
		rt.qrCancel = nil
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
	cli := rt.client
	if cli != nil {
		rt.mu.Lock()
		if rt.qrCancel != nil {
			rt.qrCancel()
			rt.qrCancel = nil
		}
		rt.mu.Unlock()
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
	resp, err := rt.client.SendMessage(ctx, jid, &waE2E.Message{Conversation: proto.String(text)})
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
	resp, err := rt.client.SendMessage(ctx, jid, &waE2E.Message{Conversation: proto.String(text)})
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
	resp, err := rt.client.SendMessage(ctx, jid, msg)
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
	resp, err := rt.client.SendMessage(ctx, jid, msg)
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
