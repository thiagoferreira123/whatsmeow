package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// WebhookSender delivers events to per-instance webhook URLs in the same shape
// the web app already expects from uazapi (header x-uazapi-secret), with simple
// retry and messageid-based de-duplication.
type WebhookSender struct {
	client    *http.Client
	mu        sync.Mutex
	seen      map[string]time.Time
	lastSweep time.Time
}

func NewWebhookSender() *WebhookSender {
	return &WebhookSender{
		client:    &http.Client{Timeout: 15 * time.Second},
		seen:      make(map[string]time.Time),
		lastSweep: time.Now(),
	}
}

// dedup reports whether id is new (not seen in the last 2h). Empty id is always
// new. Expired entries are swept at most every 10 minutes — sweeping on every
// message would make high-volume traffic quadratic over the 2h window.
func (ws *WebhookSender) dedup(id string) bool {
	if id == "" {
		return true
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()
	now := time.Now()
	if now.Sub(ws.lastSweep) > 10*time.Minute {
		for k, t := range ws.seen {
			if now.Sub(t) > 2*time.Hour {
				delete(ws.seen, k)
			}
		}
		ws.lastSweep = now
	}
	if _, ok := ws.seen[id]; ok {
		return false
	}
	ws.seen[id] = now
	return true
}

// deliver POSTs the payload to url asynchronously, retrying up to 3 times.
func (ws *WebhookSender) deliver(url, secret string, payload any) {
	if url == "" {
		return
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	go func() {
		for attempt := 0; attempt < 3; attempt++ {
			req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
			if err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/json")
			if secret != "" {
				req.Header.Set("x-uazapi-secret", secret)
			}
			resp, err := ws.client.Do(req)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode < 500 {
					return // delivered (or a non-retryable 4xx)
				}
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
	}()
}

// GlobalWebhook is the single, account-wide webhook (WhatsApp Cloud API style).
type GlobalWebhook struct {
	URL         string `json:"url"`
	VerifyToken string `json:"verifyToken"`
	AppSecret   string `json:"appSecret"`
	Enabled     bool   `json:"enabled"`
}

const settingGlobalWebhook = "global_webhook"

// loadGlobalWebhook hydrates the cached global webhook from the DB, falling back
// to env defaults the first time.
func (m *Manager) loadGlobalWebhook() {
	gw := GlobalWebhook{
		URL:         m.cfg.WebhookURL,
		VerifyToken: m.cfg.WebhookVerifyToken,
		AppSecret:   m.cfg.WebhookSecret,
		Enabled:     m.cfg.WebhookURL != "",
	}
	if raw, err := m.store.GetSetting(settingGlobalWebhook); err == nil && raw != "" {
		_ = json.Unmarshal([]byte(raw), &gw)
	}
	m.gwMu.Lock()
	m.gw = gw
	m.gwMu.Unlock()
}

func (m *Manager) GetGlobalWebhook() GlobalWebhook {
	m.gwMu.RLock()
	defer m.gwMu.RUnlock()
	return m.gw
}

func (m *Manager) SetGlobalWebhook(gw GlobalWebhook) error {
	raw, err := json.Marshal(gw)
	if err != nil {
		return err
	}
	if err := m.store.SetSetting(settingGlobalWebhook, string(raw)); err != nil {
		return err
	}
	m.gwMu.Lock()
	m.gw = gw
	m.gwMu.Unlock()
	return nil
}

// deliverCloudAPI POSTs a WhatsApp Cloud API-shaped payload to the global webhook,
// signed with X-Hub-Signature-256 (HMAC-SHA256 of the body using appSecret).
func (ws *WebhookSender) deliverCloudAPI(url, appSecret string, payload any) {
	if url == "" {
		return
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	sig := ""
	if appSecret != "" {
		mac := hmac.New(sha256.New, []byte(appSecret))
		mac.Write(body)
		sig = "sha256=" + hex.EncodeToString(mac.Sum(nil))
	}
	go func() {
		for attempt := 0; attempt < 3; attempt++ {
			req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
			if err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("User-Agent", "whatsmeow-cloudapi/1.0")
			if sig != "" {
				req.Header.Set("X-Hub-Signature-256", sig)
			}
			resp, err := ws.client.Do(req)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode < 500 {
					return
				}
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
	}()
}

// verifyRemoteWebhook performs the Cloud API GET handshake against url, returning
// true if it echoes back the challenge for the given verify token.
func (ws *WebhookSender) verifyRemoteWebhook(url, verifyToken string) bool {
	if url == "" {
		return false
	}
	challenge := "wm_" + verifyToken
	sep := "?"
	if bytes.ContainsRune([]byte(url), '?') {
		sep = "&"
	}
	full := url + sep + "hub.mode=subscribe&hub.verify_token=" + verifyToken + "&hub.challenge=" + challenge
	req, err := http.NewRequest(http.MethodGet, full, nil)
	if err != nil {
		return false
	}
	resp, err := ws.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	buf := make([]byte, len(challenge)+8)
	n, _ := resp.Body.Read(buf)
	return resp.StatusCode == 200 && string(buf[:n]) == challenge
}

// webhookSecretFor returns the instance's own secret, falling back to the global default.
func webhookSecretFor(in Instance, cfg Config) string {
	if in.WebhookSecret != "" {
		return in.WebhookSecret
	}
	return cfg.WebhookSecret
}

func messageWebhookPayload(in Instance, message map[string]any) map[string]any {
	return map[string]any{
		"EventType":    "messages",
		"event":        "messages",
		"token":        in.Token,
		"instanceName": in.Name,
		"instance": map[string]any{
			"id":           in.ID,
			"token":        in.Token,
			"status":       "connected",
			"adminField01": in.AdminField01,
		},
		"message": message,
	}
}

func connectionWebhookPayload(in Instance, status, reason string) map[string]any {
	return map[string]any{
		"EventType":    "connection",
		"event":        "connection",
		"token":        in.Token,
		"instanceName": in.Name,
		"instance": map[string]any{
			"id":                   in.ID,
			"token":                in.Token,
			"status":               status,
			"adminField01":         in.AdminField01,
			"lastDisconnectReason": reason,
		},
	}
}
