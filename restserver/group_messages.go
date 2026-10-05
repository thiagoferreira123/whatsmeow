package main

import (
	"context"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// Grupos existem só para o painel de atendimento (instância agendamento_bot):
// a equipe lê e responde pelo painel. Nenhum auto-reply, nenhum webhook global,
// nenhuma janela de consentimento para integrantes, nenhuma colheita para a
// base de conhecimento. Ecos fromMe (digitados no telefone ou enviados pelo
// painel) também seguem, para a conversa ficar completa no painel.

// O assunto do grupo muda raramente; uma consulta que falhou é repetida logo,
// para o painel não ficar horas mostrando o grupo só pelo id.
const (
	groupNameTTL      = 6 * time.Hour
	groupNameRetryTTL = 5 * time.Minute
)

type groupNameEntry struct {
	name    string
	expires time.Time
}

type groupNameCache struct {
	mu    sync.Mutex
	names map[string]groupNameEntry
}

func (c *groupNameCache) get(jid types.JID) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.names[jid.String()]
	if !ok || time.Now().After(entry.expires) {
		return "", false
	}
	return entry.name, true
}

func (c *groupNameCache) put(jid types.JID, name string, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.names == nil {
		c.names = map[string]groupNameEntry{}
	}
	c.names[jid.String()] = groupNameEntry{name: name, expires: time.Now().Add(ttl)}
}

func (m *Manager) onGroupMessage(instanceID string, v *events.Message) {
	rt := m.get(instanceID)
	if rt == nil {
		return
	}
	in := rt.metaCopy()
	if in.Name != "agendamento_bot" || in.WebhookURL == "" || !in.WebhookEnabled {
		return
	}
	// O mesmo evento chega em todas as sessões pareadas do número; entrega uma vez.
	if !m.webhooks.dedup("group:" + v.Info.ID) {
		return
	}
	sentByAPI := v.Info.IsFromMe && m.wasSentByAPI(v.Info.ID)
	if !v.Info.IsFromMe {
		// Uma mensagem no grupo abre a janela de atendimento DO GRUPO (chave = id do
		// grupo), como uma mensagem direta abre a do contato. Quem escreveu não ganha
		// janela própria: ninguém falou com a conta em particular.
		if key := permissionKey(v.Info.Chat.User); key != "" {
			if err := m.store.RecordInbound(instanceID, key, time.Now()); err != nil {
				m.log.Warnf("instance %s: failed to persist group service window: %v", instanceID, err)
			}
		}
	}
	available := m.savePanelMedia(in, v.Info.ID, v.Message, v.Info.Timestamp)
	msg := groupWebhookMessage(v, sentByAPI, m.groupName(rt, v.Info.Chat), available)
	m.webhooks.deliver(in.WebhookURL, webhookSecretFor(in, m.cfg), messageWebhookPayload(in, msg))
}

// groupWebhookMessage é a mensagem do webhook por instância para um grupo: o chat
// é o grupo; sender_pn, sender e pushName identificam quem escreveu.
func groupWebhookMessage(v *events.Message, sentByAPI bool, groupName string, mediaAvailable bool) map[string]any {
	senderPN, senderLID := resolveSender(v.Info)
	return map[string]any{
		"messageid":      v.Info.ID,
		"timestamp":      v.Info.Timestamp.UTC().Format(time.RFC3339Nano),
		"text":           historyText(v.Message),
		"fromMe":         v.Info.IsFromMe,
		"wasSentByApi":   sentByAPI,
		"isGroup":        true,
		"sender_pn":      senderPN,
		"sender":         senderLID,
		"chatid":         v.Info.Chat.String(),
		"pushName":       v.Info.PushName,
		"groupName":      groupName,
		"media":          historyMedia(v.Message),
		"mediaName":      panelMediaName(v.Message),
		"mediaAvailable": mediaAvailable,
	}
}

// groupName devolve o assunto do grupo, consultando o WhatsApp no máximo uma vez
// por TTL. Sem cliente (ou em falha) devolve "" e o painel mostra o grupo pelo id
// até a próxima mensagem.
func (m *Manager) groupName(rt *instanceRuntime, jid types.JID) string {
	if name, ok := m.groupNames.get(jid); ok {
		return name
	}
	if rt == nil || rt.client == nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	info, err := rt.client.GetGroupInfo(ctx, jid)
	if err != nil || info == nil {
		m.groupNames.put(jid, "", groupNameRetryTTL)
		return ""
	}
	m.groupNames.put(jid, info.Name, groupNameTTL)
	return info.Name
}

// onGroupInfo acompanha a troca de assunto, para o painel não exibir o nome antigo até o TTL vencer.
func (m *Manager) onGroupInfo(v *events.GroupInfo) {
	if v == nil || v.Name == nil {
		return
	}
	m.groupNames.put(v.JID, v.Name.Name, groupNameTTL)
}
