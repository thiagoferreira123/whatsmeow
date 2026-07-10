package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// apiError is an error carrying an HTTP status code.
type apiError struct {
	Status     int
	Msg        string
	RetryAfter int
}

func (e *apiError) Error() string { return e.Msg }

type Handlers struct {
	mgr *Manager
	cfg Config
}

const serviceVersion = "audit-v4-diagnostics"

func NewHandlers(mgr *Manager, cfg Config) *Handlers {
	return &Handlers{mgr: mgr, cfg: cfg}
}

func (h *Handlers) Router() http.Handler {
	mux := http.NewServeMux()

	// Management UI (served same-origin so the browser has no CORS issues).
	mux.HandleFunc("GET /{$}", h.serveUI)
	mux.HandleFunc("GET /ui", h.serveUI)

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":              true,
			"service":         "whatsmeow-restserver",
			"version":         serviceVersion,
			"outboundSafety":  true,
			"persistentQueue": true,
			"runtimeRecovery": true,
		})
	})
	mux.HandleFunc("GET /metrics", h.metrics)

	// Single global webhook (WhatsApp Cloud API style).
	mux.HandleFunc("GET /webhook", h.verifyWebhook)           // public verification handshake
	mux.HandleFunc("GET /webhook/config", h.getWebhookConfig) // panel (auth)
	mux.HandleFunc("POST /webhook/config", h.setWebhookConfig)

	mux.HandleFunc("POST /instances", h.createInstance)
	mux.HandleFunc("GET /instances", h.listInstances)
	mux.HandleFunc("GET /instances/{id}", h.getInstance)
	mux.HandleFunc("DELETE /instances/{id}", h.deleteInstance)
	mux.HandleFunc("GET /instances/{id}/qr", h.getQR)
	mux.HandleFunc("GET /instances/{id}/qr.png", h.getQRPNG)
	mux.HandleFunc("GET /instances/{id}/status", h.getStatus)
	mux.HandleFunc("GET /instances/{id}/profile", h.getProfile)
	mux.HandleFunc("GET /instances/{id}/contact", h.getContact)
	mux.HandleFunc("POST /instances/{id}/send/text", h.sendText)
	mux.HandleFunc("POST /instances/{id}/send/media", h.sendMedia)
	mux.HandleFunc("GET /instances/{id}/consents/{number}", h.getConsent)
	mux.HandleFunc("POST /instances/{id}/consents", h.grantConsent)
	mux.HandleFunc("POST /instances/{id}/consents/revoke", h.revokeConsent)
	mux.HandleFunc("POST /instances/{id}/webhook", h.setWebhook)
	mux.HandleFunc("POST /instances/{id}/disconnect", h.disconnect)
	mux.HandleFunc("POST /instances/{id}/hibernate", h.hibernate)
	mux.HandleFunc("POST /instances/{id}/resume", h.resume)
	mux.HandleFunc("POST /instances/{id}/reset", h.resetRuntime)
	mux.HandleFunc("GET /instances/{id}/queue", h.getQueue)
	mux.HandleFunc("DELETE /instances/{id}/queue", h.clearQueue)
	mux.HandleFunc("GET /instances/{id}/logs", h.getInstanceLogs)

	// uazapi wire-compat layer (header auth admintoken/token; see uazapi_compat.go).
	h.registerUazapiCompat(mux)

	return h.withAuth(mux)
}

// --- middleware ---

// withAuth checks a global API key unless ADMIN_API_KEY is empty. /health is open.
func (h *Handlers) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.cfg.AdminAPIKey == "" || r.URL.Path == "/health" || r.URL.Path == "/" || r.URL.Path == "/ui" || r.URL.Path == "/webhook" || isUazapiCompatPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		if h.providedKey(r) != h.cfg.AdminAPIKey {
			writeErr(w, http.StatusUnauthorized, "invalid or missing API key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h *Handlers) providedKey(r *http.Request) string {
	if a := r.Header.Get("Authorization"); a != "" {
		const p = "Bearer "
		if len(a) > len(p) && a[:len(p)] == p {
			return a[len(p):]
		}
		return a
	}
	if t := r.Header.Get("token"); t != "" {
		return t
	}
	return r.Header.Get("apikey")
}

// --- handlers ---

func (h *Handlers) createInstance(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name          string `json:"name"`
		AdminField01  string `json:"adminField01"`
		WebhookURL    string `json:"webhookUrl"`
		WebhookSecret string `json:"webhookSecret"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	if body.Name == "" {
		writeErr(w, http.StatusBadRequest, "name is required")
		return
	}
	in, err := h.mgr.Create(body.Name, body.AdminField01, body.WebhookURL, body.WebhookSecret)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, in)
}

func (h *Handlers) listInstances(w http.ResponseWriter, r *http.Request) {
	list, err := h.mgr.List()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (h *Handlers) getInstance(w http.ResponseWriter, r *http.Request) {
	in, err := h.mgr.Get(r.PathValue("id"))
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, in)
}

func (h *Handlers) deleteInstance(w http.ResponseWriter, r *http.Request) {
	if handleErr(w, h.mgr.Delete(r.Context(), r.PathValue("id"))) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) getQR(w http.ResponseWriter, r *http.Request) {
	res, err := h.mgr.QR(r.PathValue("id"))
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *Handlers) getQRPNG(w http.ResponseWriter, r *http.Request) {
	png, err := h.mgr.QRPNG(r.PathValue("id"))
	if handleErr(w, err) {
		return
	}
	if png == nil {
		writeErr(w, http.StatusConflict, "no QR available (already connected or pairing not started)")
		return
	}
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(png)
}

func (h *Handlers) getStatus(w http.ResponseWriter, r *http.Request) {
	res, err := h.mgr.StatusDetail(r.PathValue("id"))
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// verifyWebhook implements the WhatsApp Cloud API verification handshake.
func (h *Handlers) verifyWebhook(w http.ResponseWriter, r *http.Request) {
	gw := h.mgr.GetGlobalWebhook()
	q := r.URL.Query()
	if q.Get("hub.mode") == "subscribe" && gw.VerifyToken != "" && q.Get("hub.verify_token") == gw.VerifyToken {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(q.Get("hub.challenge")))
		return
	}
	w.WriteHeader(http.StatusForbidden)
}

func (h *Handlers) getWebhookConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.mgr.GetGlobalWebhook())
}

func (h *Handlers) setWebhookConfig(w http.ResponseWriter, r *http.Request) {
	var gw GlobalWebhook
	if !readJSON(w, r, &gw) {
		return
	}
	if err := h.mgr.SetGlobalWebhook(gw); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	verified := false
	if gw.Enabled && gw.URL != "" && gw.VerifyToken != "" {
		verified = h.mgr.webhooks.verifyRemoteWebhook(gw.URL, gw.VerifyToken)
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "verified": verified})
}

func (h *Handlers) getProfile(w http.ResponseWriter, r *http.Request) {
	in, err := h.mgr.OwnerProfile(r.Context(), r.PathValue("id"))
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, in)
}

func (h *Handlers) getContact(w http.ResponseWriter, r *http.Request) {
	number := r.URL.Query().Get("number")
	if number == "" {
		writeErr(w, http.StatusBadRequest, "query param 'number' is required")
		return
	}
	res, err := h.mgr.ContactProfile(r.Context(), r.PathValue("id"), number)
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *Handlers) sendText(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Number         string `json:"number"`
		Text           string `json:"text"`
		Async          bool   `json:"async"`
		IdempotencyKey string `json:"idempotencyKey"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	if body.Number == "" || body.Text == "" {
		writeErr(w, http.StatusBadRequest, "number and text are required")
		return
	}
	if body.Async || r.URL.Query().Get("async") == "true" {
		key := body.IdempotencyKey
		if key == "" {
			key = r.Header.Get("Idempotency-Key")
		}
		job, created, err := h.mgr.EnqueueTextFrom(r.PathValue("id"), body.Number, body.Text, key, "native_api")
		if handleErr(w, err) {
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"id": job.ID, "status": job.Status, "created": created})
		return
	}
	id, err := h.mgr.SendText(withSendAudit(r.Context(), "native_api", ""), r.PathValue("id"), body.Number, body.Text)
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": "sent"})
}

func (h *Handlers) sendMedia(w http.ResponseWriter, r *http.Request) {
	// File upload: multipart/form-data with a "file" part.
	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
		h.sendMediaUpload(w, r)
		return
	}
	// URL / base64 / data URI via JSON.
	var body struct {
		Number         string `json:"number"`
		Type           string `json:"type"`
		File           string `json:"file"`
		Text           string `json:"text"`
		FileName       string `json:"fileName"`
		Async          bool   `json:"async"`
		IdempotencyKey string `json:"idempotencyKey"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	if body.Number == "" || body.File == "" {
		writeErr(w, http.StatusBadRequest, "number and file are required")
		return
	}
	if body.Async || r.URL.Query().Get("async") == "true" {
		key := body.IdempotencyKey
		if key == "" {
			key = r.Header.Get("Idempotency-Key")
		}
		job, created, err := h.mgr.EnqueueMediaFrom(r.PathValue("id"), queuedMediaPayload{
			Number: body.Number, Type: body.Type, File: body.File, Text: body.Text, FileName: body.FileName,
		}, key, "native_api")
		if handleErr(w, err) {
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"id": job.ID, "status": job.Status, "created": created})
		return
	}
	id, err := h.mgr.SendMedia(withSendAudit(r.Context(), "native_api", ""), r.PathValue("id"), body.Number, body.Type, body.File, body.Text, body.FileName)
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": "sent"})
}

func (h *Handlers) sendMediaUpload(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("async") == "true" {
		writeErr(w, http.StatusBadRequest, "async multipart upload is not supported; use JSON URL/base64 media")
		return
	}
	if err := r.ParseMultipartForm(64 << 20); err != nil { // 64 MB in memory, rest spooled to disk
		writeErr(w, http.StatusBadRequest, "invalid multipart form: "+err.Error())
		return
	}
	number := r.FormValue("number")
	if number == "" {
		writeErr(w, http.StatusBadRequest, "number is required")
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		writeErr(w, http.StatusBadRequest, "missing file field: "+err.Error())
		return
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "failed to read upload: "+err.Error())
		return
	}
	fileName := r.FormValue("fileName")
	if fileName == "" && header != nil {
		fileName = header.Filename
	}
	mime := ""
	if header != nil {
		mime = header.Header.Get("Content-Type")
	}
	id, err := h.mgr.SendMediaBytes(withSendAudit(r.Context(), "native_api", ""), r.PathValue("id"), number, r.FormValue("type"), data, mime, r.FormValue("text"), fileName)
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": "sent"})
}

func (h *Handlers) getConsent(w http.ResponseWriter, r *http.Request) {
	p, err := h.mgr.GetConsent(r.PathValue("id"), r.PathValue("number"))
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (h *Handlers) grantConsent(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Number string `json:"number"`
		Source string `json:"source"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	p, err := h.mgr.GrantConsent(r.PathValue("id"), body.Number, body.Source)
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (h *Handlers) revokeConsent(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Number string `json:"number"`
		Source string `json:"source"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	p, err := h.mgr.RevokeConsent(r.PathValue("id"), body.Number, body.Source)
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (h *Handlers) setWebhook(w http.ResponseWriter, r *http.Request) {
	var body struct {
		URL     string `json:"url"`
		Secret  string `json:"secret"`
		Events  string `json:"events"`
		Enabled *bool  `json:"enabled"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	enabled := true
	if body.Enabled != nil {
		enabled = *body.Enabled
	}
	if handleErr(w, h.mgr.SetWebhook(r.PathValue("id"), body.URL, body.Secret, body.Events, enabled)) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (h *Handlers) disconnect(w http.ResponseWriter, r *http.Request) {
	if handleErr(w, h.mgr.Disconnect(r.PathValue("id"))) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) hibernate(w http.ResponseWriter, r *http.Request) {
	if handleErr(w, h.mgr.Disconnect(r.PathValue("id"))) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "hibernated"})
}

func (h *Handlers) resume(w http.ResponseWriter, r *http.Request) {
	if handleErr(w, h.mgr.Resume(r.PathValue("id"))) {
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "connecting"})
}

func (h *Handlers) resetRuntime(w http.ResponseWriter, r *http.Request) {
	result, err := h.mgr.ResetRuntime(r.PathValue("id"))
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (h *Handlers) getQueue(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if h.mgr.get(id) == nil {
		handleErr(w, errNotFound)
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	jobs, err := h.mgr.store.ListQueue(id, r.URL.Query().Get("status"), limit)
	if handleErr(w, err) {
		return
	}
	summary, err := h.mgr.store.QueueSummary(id)
	if handleErr(w, err) {
		return
	}
	rt := h.mgr.get(id)
	sessionReady := false
	if rt != nil {
		rt.mu.RLock()
		cli := rt.client
		sessionReady = cli != nil && cli.IsConnected() && cli.IsLoggedIn()
		rt.mu.RUnlock()
	}
	writeJSON(w, http.StatusOK, map[string]any{"summary": summary, "jobs": jobs, "sessionReady": sessionReady})
}

func (h *Handlers) clearQueue(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if h.mgr.get(id) == nil {
		handleErr(w, errNotFound)
		return
	}
	canceled, err := h.mgr.store.CancelQueue(id)
	if handleErr(w, err) {
		return
	}
	h.mgr.auditInstance(id, logCategoryQueue, "queue_canceled", "warning", InstanceLog{
		Status: queueCanceled, Source: "operator", Details: map[string]any{"canceled": canceled},
	})
	writeJSON(w, http.StatusOK, map[string]any{"canceled": canceled})
}

func (h *Handlers) getInstanceLogs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if h.mgr.get(id) == nil {
		handleErr(w, errNotFound)
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	before, _ := strconv.ParseInt(r.URL.Query().Get("before"), 10, 64)
	category := strings.TrimSpace(r.URL.Query().Get("category"))
	level := strings.TrimSpace(r.URL.Query().Get("level"))
	if category != "" && category != logCategoryConnection && category != logCategorySend && category != logCategoryQueue && category != logCategorySystem {
		writeErr(w, http.StatusBadRequest, "invalid log category")
		return
	}
	if level != "" && level != "info" && level != "warning" && level != "error" {
		writeErr(w, http.StatusBadRequest, "invalid log level")
		return
	}
	logs, err := h.mgr.store.ListInstanceLogs(id, InstanceLogQuery{
		BeforeID: before, Limit: limit, Category: category, Level: level,
	})
	if handleErr(w, err) {
		return
	}
	var nextBefore int64
	if len(logs) > 0 {
		nextBefore = logs[len(logs)-1].ID
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"logs": logs, "nextBefore": nextBefore, "retentionDays": h.cfg.InstanceLogRetentionDays,
	})
}

func (h *Handlers) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(h.mgr.metricsText()))
}

// --- json helpers ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}

func readJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

// handleErr maps known errors to HTTP responses. Returns true if it wrote one.
func handleErr(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errNotFound) {
		writeErr(w, http.StatusNotFound, "instance not found")
		return true
	}
	var ae *apiError
	if errors.As(err, &ae) {
		if ae.RetryAfter > 0 {
			w.Header().Set("Retry-After", fmt.Sprintf("%d", ae.RetryAfter))
		}
		writeErr(w, ae.Status, ae.Msg)
		return true
	}
	writeErr(w, http.StatusInternalServerError, err.Error())
	return true
}
