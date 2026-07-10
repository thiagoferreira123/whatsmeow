package main

import (
	"net/http"
	"strings"
)

// uazapi wire-compat layer: lets the DietSystem backend treat this service as a
// drop-in uazapi (same paths /instance/* /send/* /webhook, header auth
// admintoken/token, uazapi-shaped responses). Outbound webhooks in uazapi format
// are emitted by events.go when an instance has a WebhookURL set (via POST /webhook).

func (h *Handlers) registerUazapiCompat(mux *http.ServeMux) {
	mux.HandleFunc("POST /instance/init", h.uzInit)
	mux.HandleFunc("POST /instance/connect", h.uzConnect)
	mux.HandleFunc("GET /instance/status", h.uzStatus)
	mux.HandleFunc("GET /instance/all", h.uzAll)
	mux.HandleFunc("POST /instance/disconnect", h.uzDisconnect)
	mux.HandleFunc("POST /instance/hibernate", h.uzDisconnect)
	mux.HandleFunc("POST /instance/resume", h.uzResume)
	mux.HandleFunc("POST /instance/reset", h.uzReset)
	mux.HandleFunc("DELETE /instance", h.uzDelete)
	mux.HandleFunc("POST /webhook", h.uzSetWebhook)
	mux.HandleFunc("POST /send/text", h.uzSendText)
	mux.HandleFunc("POST /send/media", h.uzSendMedia)
	mux.HandleFunc("GET /message/async", h.uzAsyncQueue)
	mux.HandleFunc("DELETE /message/async", h.uzClearAsyncQueue)
}

// isUazapiCompatPath: these endpoints do their own admintoken/token auth, so they
// bypass the global Bearer middleware. Must NOT match the native "/instances" (plural).
func isUazapiCompatPath(p string) bool {
	if p == "/instance" || p == "/webhook" {
		return true
	}
	return strings.HasPrefix(p, "/instance/") || strings.HasPrefix(p, "/send/") || strings.HasPrefix(p, "/message/")
}

func (h *Handlers) uzAdminOK(r *http.Request) bool {
	if h.cfg.AdminAPIKey == "" {
		return true
	}
	return r.Header.Get("admintoken") == h.cfg.AdminAPIKey
}

func (h *Handlers) uzByToken(w http.ResponseWriter, r *http.Request) (Instance, bool) {
	tok := r.Header.Get("token")
	if tok == "" {
		writeErr(w, http.StatusUnauthorized, "missing token header")
		return Instance{}, false
	}
	in, err := h.mgr.store.GetByToken(tok)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, "invalid token")
		return Instance{}, false
	}
	return in, true
}

// uzInstanceObj renders an Instance in uazapi shape (live status merged).
func (h *Handlers) uzInstanceObj(in Instance) map[string]any {
	status := in.Status
	var detail map[string]any
	if sd, err := h.mgr.StatusDetail(in.ID); err == nil {
		detail = sd
		if s, ok := sd["status"].(string); ok {
			status = s
		}
	}
	obj := map[string]any{
		"id":             in.ID,
		"token":          in.Token,
		"name":           in.Name,
		"status":         status,
		"adminField01":   in.AdminField01,
		"profileName":    in.ProfileName,
		"profilePicUrl":  in.ProfilePicUrl,
		"isBusiness":     in.IsBusiness,
		"owner":          in.Owner,
		"created":        in.CreatedAt,
		"updated":        in.UpdatedAt,
		"qrcode":         "",
		"paircode":       "",
		"lastDisconnect": in.LastDisconnectReason,
	}
	for _, key := range []string{"connected", "loggedIn", "hibernated", "conflicted", "resetting", "lastResetAt", "sendingBlockedUntil", "queue"} {
		if detail != nil {
			if value, ok := detail[key]; ok {
				obj[key] = value
			}
		}
	}
	return obj
}

// uzInstanceWithQR adds the current QR (data URI) + status, starting pairing if needed.
func (h *Handlers) uzInstanceWithQR(in Instance) map[string]any {
	obj := h.uzInstanceObj(in)
	if res, err := h.mgr.QR(in.ID); err == nil {
		if s, ok := res["status"].(string); ok {
			obj["status"] = s
		}
		if q, ok := res["qrcode"].(string); ok {
			obj["qrcode"] = q
		}
	}
	return obj
}

func (h *Handlers) uzInit(w http.ResponseWriter, r *http.Request) {
	if !h.uzAdminOK(r) {
		writeErr(w, http.StatusUnauthorized, "invalid admintoken")
		return
	}
	var body struct {
		Name         string `json:"name"`
		AdminField01 string `json:"adminField01"`
		SystemName   string `json:"systemName"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	if body.Name == "" {
		writeErr(w, http.StatusBadRequest, "name is required")
		return
	}
	in, err := h.mgr.Create(body.Name, body.AdminField01, "", "")
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"instance": h.uzInstanceObj(in), "token": in.Token})
}

func (h *Handlers) uzConnect(w http.ResponseWriter, r *http.Request) {
	in, ok := h.uzByToken(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"instance": h.uzInstanceWithQR(in)})
}

func (h *Handlers) uzStatus(w http.ResponseWriter, r *http.Request) {
	in, ok := h.uzByToken(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"instance": h.uzInstanceObj(in)})
}

func (h *Handlers) uzAll(w http.ResponseWriter, r *http.Request) {
	if !h.uzAdminOK(r) {
		writeErr(w, http.StatusUnauthorized, "invalid admintoken")
		return
	}
	list, err := h.mgr.List()
	if handleErr(w, err) {
		return
	}
	out := make([]map[string]any, 0, len(list))
	for _, in := range list {
		out = append(out, h.uzInstanceObj(in))
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handlers) uzDisconnect(w http.ResponseWriter, r *http.Request) {
	in, ok := h.uzByToken(w, r)
	if !ok {
		return
	}
	if handleErr(w, h.mgr.Disconnect(in.ID)) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "hibernated"})
}

func (h *Handlers) uzResume(w http.ResponseWriter, r *http.Request) {
	in, ok := h.uzByToken(w, r)
	if !ok {
		return
	}
	if handleErr(w, h.mgr.Resume(in.ID)) {
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "connecting"})
}

func (h *Handlers) uzReset(w http.ResponseWriter, r *http.Request) {
	in, ok := h.uzByToken(w, r)
	if !ok {
		return
	}
	result, err := h.mgr.ResetRuntime(in.ID)
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (h *Handlers) uzDelete(w http.ResponseWriter, r *http.Request) {
	in, ok := h.uzByToken(w, r)
	if !ok {
		return
	}
	if handleErr(w, h.mgr.Delete(r.Context(), in.ID)) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "deleted"})
}

func (h *Handlers) uzSetWebhook(w http.ResponseWriter, r *http.Request) {
	in, ok := h.uzByToken(w, r)
	if !ok {
		return
	}
	var body struct {
		URL     string   `json:"url"`
		Enabled *bool    `json:"enabled"`
		Events  []string `json:"events"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	enabled := true
	if body.Enabled != nil {
		enabled = *body.Enabled
	}
	if handleErr(w, h.mgr.SetWebhook(in.ID, body.URL, "", strings.Join(body.Events, ","), enabled)) {
		return
	}
	writeJSON(w, http.StatusOK, []map[string]any{{"url": body.URL, "enabled": enabled, "events": body.Events}})
}

func (h *Handlers) uzSendText(w http.ResponseWriter, r *http.Request) {
	in, ok := h.uzByToken(w, r)
	if !ok {
		return
	}
	var body struct {
		Number         string `json:"number"`
		Text           string `json:"text"`
		Async          bool   `json:"async"`
		IdempotencyKey string `json:"idempotencyKey"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	if body.Async {
		key := body.IdempotencyKey
		if key == "" {
			key = r.Header.Get("Idempotency-Key")
		}
		job, created, err := h.mgr.EnqueueTextFrom(in.ID, body.Number, body.Text, key, "uazapi_compat")
		if handleErr(w, err) {
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"id": job.ID, "status": job.Status, "created": created})
		return
	}
	id, err := h.mgr.SendText(withSendAudit(r.Context(), "uazapi_compat", ""), in.ID, body.Number, body.Text)
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": "sent"})
}

func (h *Handlers) uzSendMedia(w http.ResponseWriter, r *http.Request) {
	in, ok := h.uzByToken(w, r)
	if !ok {
		return
	}
	var body struct {
		Number         string `json:"number"`
		Type           string `json:"type"`
		File           string `json:"file"`
		Text           string `json:"text"`
		DocName        string `json:"docName"`
		Async          bool   `json:"async"`
		IdempotencyKey string `json:"idempotencyKey"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	if body.Async {
		key := body.IdempotencyKey
		if key == "" {
			key = r.Header.Get("Idempotency-Key")
		}
		job, created, err := h.mgr.EnqueueMediaFrom(in.ID, queuedMediaPayload{
			Number: body.Number, Type: body.Type, File: body.File, Text: body.Text, FileName: body.DocName,
		}, key, "uazapi_compat")
		if handleErr(w, err) {
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"id": job.ID, "status": job.Status, "created": created})
		return
	}
	id, err := h.mgr.SendMedia(withSendAudit(r.Context(), "uazapi_compat", ""), in.ID, body.Number, body.Type, body.File, body.Text, body.DocName)
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": "sent"})
}

func (h *Handlers) uzAsyncQueue(w http.ResponseWriter, r *http.Request) {
	in, ok := h.uzByToken(w, r)
	if !ok {
		return
	}
	summary, err := h.mgr.store.QueueSummary(in.ID)
	if handleErr(w, err) {
		return
	}
	rt := h.mgr.get(in.ID)
	sessionReady := false
	resetting := false
	if rt != nil {
		rt.mu.RLock()
		cli := rt.client
		sessionReady = cli != nil && cli.IsConnected() && cli.IsLoggedIn()
		resetting = rt.resetting
		rt.mu.RUnlock()
	}
	status := "idle"
	if pending, _ := summary["pending"].(int64); pending > 0 {
		if sessionReady {
			status = "queued"
		} else {
			status = "waiting_connection"
		}
	}
	processingNow := false
	if counts, ok := summary["counts"].(map[string]int64); ok {
		processingNow = counts[queueProcessing] > 0
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": status, "pending": summary["pending"], "sessionReady": sessionReady,
		"processingNow": processingNow, "acceptingNewMessages": true, "resetting": resetting,
	})
}

func (h *Handlers) uzClearAsyncQueue(w http.ResponseWriter, r *http.Request) {
	in, ok := h.uzByToken(w, r)
	if !ok {
		return
	}
	canceled, err := h.mgr.store.CancelQueue(in.ID)
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"canceled": canceled})
}
