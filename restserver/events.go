package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// intentRe matches a confirmation reply: the first non-space char is 1 or 2,
// not immediately followed by another digit (so "10"/"12" are rejected but
// "1", " 2 ", "1.", "2) ok" are accepted).
var intentRe = regexp.MustCompile(`^\s*([12])(\D|$)`)

var optOutWords = map[string]struct{}{
	"stop": {}, "sair": {}, "parar": {}, "remover": {}, "descadastrar": {},
}

func isOptOut(text string) bool {
	_, ok := optOutWords[strings.ToLower(strings.TrimSpace(text))]
	return ok
}

func parseIntent(text string) string {
	mt := intentRe.FindStringSubmatch(text)
	if mt == nil {
		return ""
	}
	return mt[1]
}

func extractText(msg *waE2E.Message) string {
	if msg == nil {
		return ""
	}
	if c := msg.GetConversation(); c != "" {
		return c
	}
	if e := msg.GetExtendedTextMessage(); e != nil {
		return e.GetText()
	}
	return ""
}

// makeHandler returns the whatsmeow event handler bound to one instance id.
func (m *Manager) makeHandler(instanceID string) func(interface{}) {
	return func(evt interface{}) {
		switch v := evt.(type) {
		case *events.Message:
			m.onMessage(instanceID, v)
		case *events.Connected:
			m.onConnected(instanceID)
		case *events.PairSuccess:
			m.onPairSuccess(instanceID, v)
		case *events.LoggedOut:
			m.onLoggedOut(instanceID, v)
		case *events.StreamReplaced:
			m.onStreamReplaced(instanceID)
		case *events.Disconnected:
			m.onDisconnected(instanceID)
		case *events.TemporaryBan:
			m.onTemporaryBan(instanceID, v)
		case *events.ClientOutdated:
			m.onClientOutdated(instanceID)
		case *events.ConnectFailure:
			m.onConnectFailure(instanceID, v)
		case *events.KeepAliveTimeout:
			m.onKeepAliveTimeout(instanceID, v)
		case *events.KeepAliveRestored:
			m.auditInstance(instanceID, logCategoryConnection, "keepalive_restored", "info", InstanceLog{
				Status: "connected", Source: "whatsapp_socket",
			})
		}
	}
}

func (m *Manager) onMessage(instanceID string, v *events.Message) {
	if v.Info.IsFromMe || v.Info.IsGroup {
		return
	}
	rt := m.get(instanceID)
	if rt == nil {
		return
	}
	text := extractText(v.Message)
	if !m.webhooks.dedup(v.Info.ID) {
		return
	}
	// Any inbound direct message opens the configured service window. Explicit
	// stop words persist a suppression that wins over that window and over the
	// SEND_REQUIRE_LOCAL_CONSENT setting.
	senderPN, _ := resolveSender(v.Info)
	permissionKeys := map[string]struct{}{}
	for _, raw := range []string{senderPN, v.Info.Sender.User, v.Info.Chat.User} {
		if key := permissionKey(raw); key != "" {
			permissionKeys[key] = struct{}{}
		}
	}
	for key := range permissionKeys {
		var err error
		if isOptOut(text) {
			err = m.store.RevokeRecipientConsent(instanceID, key, "inbound_opt_out", time.Now())
		} else {
			err = m.store.RecordInbound(instanceID, key, time.Now())
		}
		if err != nil {
			m.log.Warnf("instance %s: failed to persist recipient permission: %v", instanceID, err)
		}
	}
	intent := parseIntent(text)

	// Built-in 1/2 appointment-confirmation auto-reply. Sent in the background
	// so a slow send never blocks this instance's event-handler queue.
	if m.cfg.AutoReplyEnabled && intent != "" {
		reply := m.cfg.AutoReplyConfirm
		if intent == "2" {
			reply = m.cfg.AutoReplyCancel
		}
		go func(chat types.JID) {
			ctx := withSendAudit(context.Background(), "auto_reply", "")
			if _, err := m.sendTextJID(ctx, instanceID, chat, reply); err != nil {
				m.log.Warnf("instance %s: auto-reply blocked or failed: %v", instanceID, err)
			}
		}(v.Info.Chat)
	}

	// Deliver to the SINGLE GLOBAL webhook (WhatsApp Cloud API format).
	gw := m.GetGlobalWebhook()
	if gw.Enabled && gw.URL != "" {
		confirmation := ""
		switch intent {
		case "1":
			confirmation = "confirmed"
		case "2":
			confirmation = "cancelled"
		}
		payload := cloudAPIMessagePayload(rt.metaCopy(), v.Info, text, confirmation)
		m.webhooks.deliverCloudAPI(gw.URL, gw.AppSecret, payload)
	}

	// Per-instance uazapi-format webhook (set via POST /webhook compat) — lets the
	// DietSystem backend consume this service exactly like uazapi (/webhooks/uazapi).
	in := rt.metaCopy()
	if in.WebhookURL != "" && in.WebhookEnabled {
		senderPN, senderLID := resolveSender(v.Info)
		msg := map[string]any{
			"messageid":    v.Info.ID,
			"text":         text,
			"fromMe":       v.Info.IsFromMe,
			"wasSentByApi": false,
			"isGroup":      v.Info.IsGroup,
			"sender_pn":    senderPN,
			"sender":       senderLID,
			"chatid":       v.Info.Chat.String(),
			"pushName":     v.Info.PushName,
		}
		m.webhooks.deliver(in.WebhookURL, webhookSecretFor(in, m.cfg), messageWebhookPayload(in, msg))
	}
}

// digitsOnly strips everything but digits (e.g. "5511...@s.whatsapp.net" -> "5511...").
func digitsOnly(s string) string { return nonDigit.ReplaceAllString(s, "") }

// cloudAPIMessagePayload builds a WhatsApp Cloud API webhook envelope for an
// incoming message. When confirmation is non-empty ("confirmed"/"cancelled"),
// it is added to the value so the receiver knows the appointment outcome.
func cloudAPIMessagePayload(in Instance, info types.MessageInfo, text, confirmation string) map[string]any {
	senderPN, _ := resolveSender(info)
	waid := digitsOnly(senderPN)
	value := map[string]any{
		"messaging_product": "whatsapp",
		"metadata":          map[string]any{"display_phone_number": in.Owner, "phone_number_id": in.ID},
		"contacts":          []any{map[string]any{"profile": map[string]any{"name": info.PushName}, "wa_id": waid}},
		"messages": []any{map[string]any{
			"from":      waid,
			"id":        info.ID,
			"timestamp": fmt.Sprintf("%d", info.Timestamp.Unix()),
			"type":      "text",
			"text":      map[string]any{"body": text},
		}},
	}
	if confirmation != "" {
		value["confirmation"] = confirmation
	}
	return map[string]any{
		"object": "whatsapp_business_account",
		"entry": []any{map[string]any{
			"id":      in.ID,
			"changes": []any{map[string]any{"field": "messages", "value": value}},
		}},
	}
}

func (m *Manager) onConnected(instanceID string) {
	rt := m.get(instanceID)
	if rt == nil {
		return
	}
	cli := rt.client
	rt.mu.Lock()
	rt.meta.Status = "connected"
	rt.loggedOut = false
	rt.paused = false
	rt.conflicted = false
	rt.resetting = false
	rt.nextConnectAt = time.Time{}
	rt.connectFails = 0
	rt.meta.LastDisconnectReason = ""
	if until := parseStoredTime(rt.meta.SendingBlockedUntil); !until.IsZero() && time.Now().After(until) {
		rt.meta.SendingBlockedUntil = ""
	}
	if cli.Store != nil && cli.Store.ID != nil {
		rt.meta.JID = cli.Store.ID.String()
		rt.meta.Owner = cli.Store.ID.User
	}
	if cli.Store != nil && cli.Store.PushName != "" {
		rt.meta.ProfileName = cli.Store.PushName
	}
	in := rt.meta
	rt.mu.Unlock()
	_ = m.store.Save(&in)
	m.auditInstance(instanceID, logCategoryConnection, "connected", "info", InstanceLog{
		Status: "connected", Source: "whatsapp_event", Details: map[string]any{"owner": in.Owner, "profileName": in.ProfileName},
	})
	// Capture name / phone / profile photo in the background (needs the connection).
	go m.captureOwnerProfile(context.Background(), instanceID)
	gw := m.GetGlobalWebhook()
	if gw.Enabled && gw.URL != "" {
		m.webhooks.deliverCloudAPI(gw.URL, gw.AppSecret, cloudAPIStatusPayload(in, "connected", ""))
	}
}

// cloudAPIStatusPayload builds a Cloud API-style envelope for an account
// connection status change (connected / disconnected).
func cloudAPIStatusPayload(in Instance, status, reason string) map[string]any {
	return map[string]any{
		"object": "whatsapp_business_account",
		"entry": []any{map[string]any{
			"id": in.ID,
			"changes": []any{map[string]any{
				"field": "account_update",
				"value": map[string]any{
					"messaging_product":    "whatsapp",
					"phone_number_id":      in.ID,
					"display_phone_number": in.Owner,
					"event":                status,
					"reason":               reason,
				},
			}},
		}},
	}
}

func (m *Manager) onPairSuccess(instanceID string, v *events.PairSuccess) {
	rt := m.get(instanceID)
	if rt == nil {
		return
	}
	rt.mu.Lock()
	rt.meta.JID = v.ID.String()
	rt.meta.Owner = v.ID.User
	rt.meta.Status = "connected"
	rt.qrCode = ""
	rt.loggedOut = false
	rt.paused = false
	rt.conflicted = false
	rt.resetting = false
	rt.nextConnectAt = time.Time{}
	rt.meta.LastDisconnectReason = ""
	if v.BusinessName != "" {
		rt.meta.ProfileName = v.BusinessName
	}
	in := rt.meta
	rt.mu.Unlock()
	_ = m.store.Save(&in)
	m.auditInstance(instanceID, logCategoryConnection, "paired", "info", InstanceLog{
		Status: "connected", Source: "qr", Details: map[string]any{"owner": in.Owner, "businessName": v.BusinessName},
	})
}

func (m *Manager) onLoggedOut(instanceID string, v *events.LoggedOut) {
	rt := m.get(instanceID)
	if rt == nil {
		return
	}
	rt.mu.Lock()
	rt.meta.Status = "disconnected"
	rt.loggedOut = true // real unlink — needs a new QR; watchdog must NOT auto-reconnect
	rt.resetting = false
	rt.meta.LastDisconnectReason = fmt.Sprintf("logged_out (onConnect=%v reason=%v)", v.OnConnect, v.Reason)
	in := rt.meta
	rt.mu.Unlock()
	_ = m.store.Save(&in)
	m.auditInstance(instanceID, logCategoryConnection, "logged_out", "error", InstanceLog{
		Status: "disconnected", Source: "whatsapp_event", Reason: in.LastDisconnectReason,
	})
	gw := m.GetGlobalWebhook()
	if gw.Enabled && gw.URL != "" {
		m.webhooks.deliverCloudAPI(gw.URL, gw.AppSecret, cloudAPIStatusPayload(in, "disconnected", in.LastDisconnectReason))
	}
}

// onStreamReplaced fires when ANOTHER client connected with the same session
// (e.g. a second process, or the number linked elsewhere). Reconnecting immediately
// would fight that client, so we pause recovery until an operator resumes or
// resets the instance after closing the competing session.
func (m *Manager) onStreamReplaced(instanceID string) {
	rt := m.get(instanceID)
	if rt == nil {
		return
	}
	m.log.Warnf("instance %s: stream replaced — the SAME session connected elsewhere. "+
		"Run only ONE process per session/DB. Automatic recovery is paused until resume/reset.", instanceID)
	rt.mu.Lock()
	rt.meta.Status = "disconnected"
	rt.meta.LastDisconnectReason = "stream_replaced (mesma sessão conectou em outro lugar)"
	rt.conflicted = true
	rt.resetting = false
	rt.nextConnectAt = time.Now().Add(5 * time.Minute)
	in := rt.meta
	rt.mu.Unlock()
	_ = m.store.Save(&in)
	m.auditInstance(instanceID, logCategoryConnection, "stream_replaced", "error", InstanceLog{
		Status: "disconnected", Source: "whatsapp_event", Reason: in.LastDisconnectReason,
	})
	gw := m.GetGlobalWebhook()
	if gw.Enabled && gw.URL != "" {
		m.webhooks.deliverCloudAPI(gw.URL, gw.AppSecret, cloudAPIStatusPayload(in, "disconnected", in.LastDisconnectReason))
	}
}

// onTemporaryBan fires when WhatsApp temp-banned the account (e.g. too many
// messages). Retrying before the ban expires only makes it worse — back off
// until it lifts and surface the reason on the instance.
func (m *Manager) onTemporaryBan(instanceID string, v *events.TemporaryBan) {
	rt := m.get(instanceID)
	if rt == nil {
		return
	}
	wait := v.Expire
	if wait <= 0 {
		wait = 30 * time.Minute
	}
	m.log.Warnf("instance %s: TEMPORARY BAN (%s) — backing off reconnect for %s", instanceID, v.String(), wait)
	blockedUntil := time.Now().Add(wait)
	rt.mu.Lock()
	rt.meta.Status = "disconnected"
	rt.meta.LastDisconnectReason = "temp_banned: " + v.String()
	rt.meta.SendingBlockedUntil = blockedUntil.UTC().Format(time.RFC3339)
	rt.resetting = false
	rt.nextConnectAt = blockedUntil
	in := rt.meta
	rt.mu.Unlock()
	_ = m.store.Save(&in)
	m.auditInstance(instanceID, logCategoryConnection, "temporary_ban", "error", InstanceLog{
		Status: "disconnected", Source: "whatsapp_event", Reason: in.LastDisconnectReason,
		Details: map[string]any{"blockedUntil": in.SendingBlockedUntil, "expiresInSeconds": int(wait.Seconds())},
	})
	gw := m.GetGlobalWebhook()
	if gw.Enabled && gw.URL != "" {
		m.webhooks.deliverCloudAPI(gw.URL, gw.AppSecret, cloudAPIStatusPayload(in, "disconnected", in.LastDisconnectReason))
	}
}

// onClientOutdated fires when WhatsApp rejects our protocol version (405).
// Reconnecting won't help until the whatsmeow library is updated — retry
// hourly just in case, and log loudly so ops knows to update/redeploy.
func (m *Manager) onClientOutdated(instanceID string) {
	rt := m.get(instanceID)
	if rt == nil {
		return
	}
	m.log.Errorf("instance %s: CLIENT OUTDATED (405) — update the whatsmeow library and redeploy. Retrying hourly.", instanceID)
	rt.mu.Lock()
	rt.meta.Status = "disconnected"
	rt.meta.LastDisconnectReason = "client_outdated (405): atualizar a lib whatsmeow e redeployar"
	rt.resetting = false
	rt.nextConnectAt = time.Now().Add(time.Hour)
	in := rt.meta
	rt.mu.Unlock()
	_ = m.store.Save(&in)
	m.auditInstance(instanceID, logCategoryConnection, "client_outdated", "error", InstanceLog{
		Status: "disconnected", Source: "whatsapp_event", Reason: in.LastDisconnectReason,
	})
}

// onConnectFailure records other server-side connect rejections so the panel
// shows why an instance is down (whatsmeow does not auto-reconnect on these;
// the watchdog keeps retrying with backoff).
func (m *Manager) onConnectFailure(instanceID string, v *events.ConnectFailure) {
	rt := m.get(instanceID)
	if rt == nil {
		return
	}
	m.log.Warnf("instance %s: connect failure %d: %s", instanceID, int(v.Reason), v.Message)
	rt.mu.Lock()
	rt.meta.Status = "disconnected"
	rt.meta.LastDisconnectReason = fmt.Sprintf("connect_failure %d: %s", int(v.Reason), v.Message)
	rt.resetting = false
	in := rt.meta
	rt.mu.Unlock()
	_ = m.store.Save(&in)
	m.auditInstance(instanceID, logCategoryConnection, "connect_failure", "error", InstanceLog{
		Status: "disconnected", Source: "whatsapp_event", Reason: in.LastDisconnectReason,
		Details: map[string]any{"code": int(v.Reason)},
	})
}

func (m *Manager) onKeepAliveTimeout(instanceID string, v *events.KeepAliveTimeout) {
	m.auditInstance(instanceID, logCategoryConnection, "keepalive_timeout", "warning", InstanceLog{
		Status: "connected", Source: "whatsapp_socket", Reason: "keepalive responses stopped",
		Details: map[string]any{"errorCount": v.ErrorCount, "lastSuccess": v.LastSuccess.UTC().Format(time.RFC3339)},
	})
}

func (m *Manager) onDisconnected(instanceID string) {
	rt := m.get(instanceID)
	if rt == nil {
		return
	}
	// Transient socket drop — whatsmeow auto-reconnects. Update in-memory status
	// only (avoid persisting/flapping the webhook on every reconnect).
	rt.mu.Lock()
	if rt.meta.Status == "connected" {
		rt.meta.Status = "disconnected"
	}
	rt.mu.Unlock()
	m.auditInstance(instanceID, logCategoryConnection, "socket_disconnected", "warning", InstanceLog{
		Status: "disconnected", Source: "whatsapp_socket", Reason: "transient socket disconnect; automatic reconnect expected",
	})
}
