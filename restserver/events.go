package main

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// intentRe matches a confirmation reply: the first non-space char is 1 or 2,
// not immediately followed by another digit (so "10"/"12" are rejected but
// "1", " 2 ", "1.", "2) ok" are accepted).
var intentRe = regexp.MustCompile(`^\s*([12])(\D|$)`)

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
	intent := parseIntent(text)

	// Built-in 1/2 appointment-confirmation auto-reply.
	if m.cfg.AutoReplyEnabled && intent != "" {
		reply := m.cfg.AutoReplyConfirm
		if intent == "2" {
			reply = m.cfg.AutoReplyCancel
		}
		_, _ = rt.client.SendMessage(context.Background(), v.Info.Chat, &waE2E.Message{Conversation: proto.String(reply)})
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
	rt.nextConnectAt = time.Time{}
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
	rt.nextConnectAt = time.Time{}
	if v.BusinessName != "" {
		rt.meta.ProfileName = v.BusinessName
	}
	in := rt.meta
	rt.mu.Unlock()
	_ = m.store.Save(&in)
}

func (m *Manager) onLoggedOut(instanceID string, v *events.LoggedOut) {
	rt := m.get(instanceID)
	if rt == nil {
		return
	}
	rt.mu.Lock()
	rt.meta.Status = "disconnected"
	rt.loggedOut = true // real unlink — needs a new QR; watchdog must NOT auto-reconnect
	rt.meta.LastDisconnectReason = fmt.Sprintf("logged_out (onConnect=%v reason=%v)", v.OnConnect, v.Reason)
	in := rt.meta
	rt.mu.Unlock()
	_ = m.store.Save(&in)
	gw := m.GetGlobalWebhook()
	if gw.Enabled && gw.URL != "" {
		m.webhooks.deliverCloudAPI(gw.URL, gw.AppSecret, cloudAPIStatusPayload(in, "disconnected", in.LastDisconnectReason))
	}
}

// onStreamReplaced fires when ANOTHER client connected with the same session
// (e.g. a second process, or the number linked elsewhere). Reconnecting immediately
// would fight that client, so we log loudly and back the watchdog off for a while.
func (m *Manager) onStreamReplaced(instanceID string) {
	rt := m.get(instanceID)
	if rt == nil {
		return
	}
	m.log.Warnf("instance %s: stream replaced — the SAME session connected elsewhere. "+
		"Run only ONE process per session/DB. Backing off reconnect for 5 min.", instanceID)
	rt.mu.Lock()
	rt.meta.Status = "disconnected"
	rt.meta.LastDisconnectReason = "stream_replaced (mesma sessão conectou em outro lugar)"
	rt.nextConnectAt = time.Now().Add(5 * time.Minute)
	in := rt.meta
	rt.mu.Unlock()
	_ = m.store.Save(&in)
	gw := m.GetGlobalWebhook()
	if gw.Enabled && gw.URL != "" {
		m.webhooks.deliverCloudAPI(gw.URL, gw.AppSecret, cloudAPIStatusPayload(in, "disconnected", in.LastDisconnectReason))
	}
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
}
