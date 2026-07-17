package main

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

// uazapi wire-compat layer: lets the DietSystem backend treat this service as a
// drop-in uazapi (same paths /instance/* /send/* /webhook, header auth
// admintoken/token, uazapi-shaped responses). Outbound webhooks in uazapi format
// are emitted by events.go when an instance has a WebhookURL set (via POST /webhook).

type uazapiWebhookConfig struct {
	ID                  string   `json:"id"`
	Enabled             bool     `json:"enabled"`
	URL                 string   `json:"url"`
	Events              []string `json:"events"`
	ExcludeMessages     []string `json:"excludeMessages"`
	AddURLEvents        bool     `json:"addUrlEvents"`
	AddURLTypesMessages bool     `json:"addUrlTypesMessages"`
}

func splitWebhookCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return []string{}
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			out = append(out, value)
		}
	}
	return out
}

func uazapiWebhookConfigFromInstance(in Instance) uazapiWebhookConfig {
	return uazapiWebhookConfig{
		ID:                  in.ID,
		Enabled:             in.WebhookEnabled,
		URL:                 in.WebhookURL,
		Events:              splitWebhookCSV(in.WebhookEvents),
		ExcludeMessages:     splitWebhookCSV(in.WebhookExcludeMessages),
		AddURLEvents:        in.WebhookAddURLEvents,
		AddURLTypesMessages: in.WebhookAddURLTypesMessages,
	}
}

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
	mux.HandleFunc("POST /sender/advanced", h.uzSenderAdvanced)
	mux.HandleFunc("GET /sender/listfolders", h.uzSenderListFolders)
	mux.HandleFunc("POST /sender/edit", h.uzSenderEdit)
}

// isUazapiCompatPath: these endpoints do their own admintoken/token auth, so they
// bypass the global Bearer middleware. Must NOT match the native "/instances" (plural).
func isUazapiCompatPath(p string) bool {
	if p == "/instance" || p == "/webhook" {
		return true
	}
	return strings.HasPrefix(p, "/instance/") || strings.HasPrefix(p, "/send/") || strings.HasPrefix(p, "/message/") || strings.HasPrefix(p, "/sender/")
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
	for _, key := range []string{"connected", "loggedIn", "hibernated", "conflicted", "resetting", "lastResetAt", "sendingBlockedUntil", "queue", "qrcode"} {
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
	in, err := h.mgr.Create(
		body.Name,
		body.AdminField01,
		h.cfg.UazapiCompatWebhookURL,
		h.cfg.WebhookSecret,
	)
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
		URL                 string   `json:"url"`
		Enabled             *bool    `json:"enabled"`
		Events              []string `json:"events"`
		ExcludeMessages     []string `json:"excludeMessages"`
		AddURLEvents        bool     `json:"addUrlEvents"`
		AddURLTypesMessages bool     `json:"addUrlTypesMessages"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	enabled := true
	if body.Enabled != nil {
		enabled = *body.Enabled
	}
	if strings.TrimSpace(body.URL) == "" {
		writeErr(w, http.StatusBadRequest, "url is required")
		return
	}
	config := uazapiWebhookConfig{
		ID:                  in.ID,
		Enabled:             enabled,
		URL:                 body.URL,
		Events:              body.Events,
		ExcludeMessages:     body.ExcludeMessages,
		AddURLEvents:        body.AddURLEvents,
		AddURLTypesMessages: body.AddURLTypesMessages,
	}
	if handleErr(w, h.mgr.SetUazapiWebhook(in.ID, config)) {
		return
	}
	writeJSON(w, http.StatusOK, []uazapiWebhookConfig{config})
}

func (h *Handlers) uzGetWebhook(w http.ResponseWriter, r *http.Request) {
	in, ok := h.uzByToken(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, []uazapiWebhookConfig{uazapiWebhookConfigFromInstance(in)})
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
		AvailableAt    string `json:"availableAt"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	if body.Async {
		availableAt := time.Now()
		if strings.TrimSpace(body.AvailableAt) != "" {
			parsed, err := time.Parse(time.RFC3339, body.AvailableAt)
			if err != nil {
				writeErr(w, http.StatusBadRequest, "availableAt must be RFC3339")
				return
			}
			if parsed.After(availableAt) {
				availableAt = parsed
			}
		}
		key := body.IdempotencyKey
		if key == "" {
			key = r.Header.Get("Idempotency-Key")
		}
		job, created, err := h.mgr.EnqueueTextAtFrom(in.ID, body.Number, body.Text, key, "uazapi_compat", availableAt)
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

func (h *Handlers) uzSenderAdvanced(w http.ResponseWriter, r *http.Request) {
	in, ok := h.uzByToken(w, r)
	if !ok {
		return
	}
	h.createBroadcastForInstance(w, r, in)
}

func (h *Handlers) createBroadcast(w http.ResponseWriter, r *http.Request) {
	in, err := h.mgr.Get(r.PathValue("id"))
	if handleErr(w, err) {
		return
	}
	h.createBroadcastForInstance(w, r, in)
}

func (h *Handlers) createBroadcastForInstance(w http.ResponseWriter, r *http.Request, in Instance) {
	var body struct {
		Messages []struct {
			Number string `json:"number"`
			Text   string `json:"text"`
			Type   string `json:"type"`
		} `json:"messages"`
		DelayMin int `json:"delayMin"`
		DelayMax int `json:"delayMax"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	if len(body.Messages) == 0 {
		writeErr(w, http.StatusBadRequest, "messages are required")
		return
	}
	if body.DelayMin < 0 || body.DelayMax < body.DelayMin {
		writeErr(w, http.StatusBadRequest, "delayMax must be greater than or equal to delayMin")
		return
	}
	for _, message := range body.Messages {
		if strings.TrimSpace(message.Number) == "" || strings.TrimSpace(message.Text) == "" || (message.Type != "" && message.Type != "text") {
			writeErr(w, http.StatusBadRequest, "each message must contain number and text with type text")
			return
		}
	}
	activeRecipients, err := h.mgr.store.ActiveBroadcastRecipients(in.ID)
	if handleErr(w, err) {
		return
	}
	for _, message := range body.Messages {
		if _, exists := activeRecipients[permissionKey(message.Number)]; exists {
			writeErr(w, http.StatusConflict, "recipient already belongs to an active broadcast campaign")
			return
		}
	}

	folderID := uuid.NewString()
	availableAt := time.Now()
	for index, message := range body.Messages {
		if index > 0 {
			delay := body.DelayMin
			if body.DelayMax > body.DelayMin {
				delay += rand.IntN(body.DelayMax - body.DelayMin + 1)
			}
			availableAt = availableAt.Add(time.Duration(delay) * time.Second)
		}
		key := "broadcast:" + folderID + ":" + fmt.Sprint(index)
		if _, _, err := h.mgr.EnqueueTextAtFrom(in.ID, message.Number, message.Text, key, "broadcast", availableAt); handleErr(w, err) {
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"folder_id": folderID, "count": len(body.Messages), "status": "queued"})
}

func (h *Handlers) uzSenderListFolders(w http.ResponseWriter, r *http.Request) {
	in, ok := h.uzByToken(w, r)
	if !ok {
		return
	}
	h.listBroadcastsForInstance(w, in)
}

func (h *Handlers) listBroadcasts(w http.ResponseWriter, r *http.Request) {
	in, err := h.mgr.Get(r.PathValue("id"))
	if handleErr(w, err) {
		return
	}
	h.listBroadcastsForInstance(w, in)
}

func (h *Handlers) listBroadcastsForInstance(w http.ResponseWriter, in Instance) {
	folders, err := h.mgr.store.ListBroadcastFolders(in.ID)
	if handleErr(w, err) {
		return
	}
	for index := range folders {
		folders[index].Owner = in.Owner
	}
	writeJSON(w, http.StatusOK, folders)
}

func (h *Handlers) uzSenderEdit(w http.ResponseWriter, r *http.Request) {
	in, ok := h.uzByToken(w, r)
	if !ok {
		return
	}
	var body struct {
		FolderID string `json:"folder_id"`
		Action   string `json:"action"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	h.controlBroadcastForInstance(w, in, body.FolderID, body.Action)
}

func (h *Handlers) controlBroadcast(w http.ResponseWriter, r *http.Request) {
	in, err := h.mgr.Get(r.PathValue("id"))
	if handleErr(w, err) {
		return
	}
	var body struct {
		Action string `json:"action"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	h.controlBroadcastForInstance(w, in, r.PathValue("folderId"), body.Action)
}

func (h *Handlers) controlBroadcastForInstance(w http.ResponseWriter, in Instance, folderID, action string) {
	if !validBroadcastFolderID(folderID) {
		writeErr(w, http.StatusBadRequest, "invalid folder_id")
		return
	}
	var (
		affected int64
		err      error
	)
	switch action {
	case "stop":
		affected, err = h.mgr.store.PauseBroadcastFolder(in.ID, folderID)
	case "continue":
		affected, err = h.mgr.store.ResumeBroadcastFolder(in.ID, folderID)
	case "delete":
		affected, err = h.mgr.store.DeleteBroadcastFolder(in.ID, folderID)
	default:
		writeErr(w, http.StatusBadRequest, "action must be stop, continue or delete")
		return
	}
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "affected": affected})
}

func validBroadcastFolderID(folderID string) bool {
	if folderID == "" {
		return false
	}
	for _, char := range folderID {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '-' && char != '_' {
			return false
		}
	}
	return true
}
