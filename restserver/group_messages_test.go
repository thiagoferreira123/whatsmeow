package main

import (
	"encoding/json"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

const (
	testGroup  = "120363012345678901"
	testMember = "5567981490781"
)

func groupEvent(id string, fromMe bool) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:     types.NewJID(testGroup, types.GroupServer),
				Sender:   types.NewJID(testMember, types.DefaultUserServer),
				IsGroup:  true,
				IsFromMe: fromMe,
			},
			ID: id, Timestamp: time.Unix(1700000000, 0), PushName: "Maria",
		},
		Message: &waE2E.Message{Conversation: proto.String("Alguém usa o DietSystem?")},
	}
}

func panelManager(t *testing.T) (*Manager, *Store) {
	t.Helper()
	m, store := testPolicyManager(t, Config{AdminAPIKey: "synthetic-master", WebhookSecret: "synthetic-hook"})
	m.log = waLog.Noop
	m.webhooks = NewWebhookSender()
	m.webhooks.agentEnqueue = m.enqueueAgentWebhook
	in := m.runtimes["instance-1"].meta
	in.Name = "agendamento_bot"
	in.WebhookURL = "https://agents.example/v1/whatsmeow"
	in.WebhookEnabled = true
	m.runtimes["instance-1"].meta = in
	return m, store
}

// queuedMessages decrypts every panel delivery persisted in the durable queue.
func queuedMessages(t *testing.T, m *Manager, store *Store) []map[string]any {
	t.Helper()
	rows, err := store.db.Query("SELECT instance_id, body FROM agent_webhook_outbox ORDER BY created_at, id")
	if err != nil {
		return nil
	}
	defer rows.Close()
	aead, err := m.agentCipher()
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for rows.Next() {
		var instanceID string
		var encrypted []byte
		if err := rows.Scan(&instanceID, &encrypted); err != nil {
			t.Fatal(err)
		}
		body, err := aead.Open(nil, encrypted[:aead.NonceSize()], encrypted[aead.NonceSize():], []byte(instanceID))
		if err != nil {
			t.Fatal(err)
		}
		var payload struct {
			Message map[string]any `json:"message"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatal(err)
		}
		out = append(out, payload.Message)
	}
	return out
}

func TestGroupWebhookMessageNamesTheGroupAndTheMember(t *testing.T) {
	msg := groupWebhookMessage(groupEvent("3EB0GROUP", false), false, "Nutris de Campo Grande", true)
	want := map[string]any{
		"messageid": "3EB0GROUP", "isGroup": true, "fromMe": false, "wasSentByApi": false,
		"chatid": testGroup + "@g.us", "sender_pn": testMember + "@s.whatsapp.net", "sender": "",
		"pushName": "Maria", "groupName": "Nutris de Campo Grande", "text": "Alguém usa o DietSystem?",
		"media": "", "mediaAvailable": true,
	}
	for key, value := range want {
		if msg[key] != value {
			t.Fatalf("%s = %v, want %v", key, msg[key], value)
		}
	}
	own := groupWebhookMessage(groupEvent("3EB0OWN", true), true, "", false)
	if own["fromMe"] != true || own["wasSentByApi"] != true || own["groupName"] != "" {
		t.Fatalf("own group echo misdescribed: %v", own)
	}
}

// Um grupo só existe para o painel: outra instância não entrega nada; a instância do
// painel entrega uma vez (eco duplicado descartado), com quem escreveu, e abre a
// janela de atendimento do grupo — nunca da integrante.
func TestGroupMessageReachesOnlyThePanelWebhookOnce(t *testing.T) {
	m, store := panelManager(t)
	other := m.runtimes["instance-1"].meta
	other.Name = "clinica-qualquer"
	m.runtimes["instance-1"].meta = other
	m.onGroupMessage("instance-1", groupEvent("other-1", false))
	if got := queuedMessages(t, m, store); len(got) != 0 {
		t.Fatalf("group delivered for an instance without the panel: %v", got)
	}

	panel := other
	panel.Name = "agendamento_bot"
	m.runtimes["instance-1"].meta = panel
	m.onGroupMessage("instance-1", groupEvent("group-1", false))
	m.onGroupMessage("instance-1", groupEvent("group-1", false))
	got := queuedMessages(t, m, store)
	if len(got) != 1 {
		t.Fatalf("expected one queued delivery, got %d", len(got))
	}
	if got[0]["isGroup"] != true || got[0]["chatid"] != testGroup+"@g.us" || got[0]["pushName"] != "Maria" || got[0]["text"] != "Alguém usa o DietSystem?" {
		t.Fatalf("group payload incomplete: %v", got[0])
	}
	if p, err := store.GetRecipientPermission("instance-1", testGroup); err != nil || p.LastInboundAt == "" {
		t.Fatalf("group service window not recorded: %v %v", p, err)
	}
	if _, err := store.GetRecipientPermission("instance-1", testMember); err == nil {
		t.Fatal("a member who wrote in the group must not get a service window of their own")
	}
}

// No chat direto o nome de exibição é redundante e fica fora da fila; no grupo é a
// única coisa que diz ao painel quem escreveu.
func TestAgentWebhookKeepsTheMemberNameOnlyForGroups(t *testing.T) {
	m, store := panelManager(t)
	in := m.runtimes["instance-1"].meta
	envelope := func(message map[string]any) map[string]any {
		return map[string]any{"instanceName": "agendamento_bot", "instance": map[string]any{"id": in.ID}, "message": message}
	}
	if !m.enqueueAgentWebhook(in.WebhookURL, envelope(map[string]any{"messageid": "direct-1", "pushName": "Maria", "text": "oi"})) {
		t.Fatal("direct message not queued")
	}
	if !m.enqueueAgentWebhook(in.WebhookURL, envelope(map[string]any{"messageid": "group-1", "isGroup": true, "pushName": "Maria", "groupName": "Nutris", "text": "oi"})) {
		t.Fatal("group message not queued")
	}
	for _, message := range queuedMessages(t, m, store) {
		_, named := message["pushName"]
		switch message["messageid"] {
		case "direct-1":
			if named {
				t.Fatal("display name persisted for a direct chat")
			}
		case "group-1":
			if message["pushName"] != "Maria" || message["groupName"] != "Nutris" {
				t.Fatalf("group message lost its names: %v", message)
			}
		default:
			t.Fatalf("unexpected message %v", message)
		}
	}
}

func TestGroupNameCacheExpiresAndFollowsSubjectChanges(t *testing.T) {
	m := &Manager{}
	jid := types.NewJID(testGroup, types.GroupServer)
	if name := m.groupName(nil, jid); name != "" {
		t.Fatalf("no client, no name; got %q", name)
	}
	m.onGroupInfo(&events.GroupInfo{JID: jid, Name: &types.GroupName{Name: "Nutris de Campo Grande"}})
	if name := m.groupName(nil, jid); name != "Nutris de Campo Grande" {
		t.Fatalf("subject change not cached; got %q", name)
	}
	m.onGroupInfo(&events.GroupInfo{JID: jid})
	if name := m.groupName(nil, jid); name != "Nutris de Campo Grande" {
		t.Fatalf("an event without a name change must keep the subject; got %q", name)
	}
	m.groupNames.put(jid, "Antigo", -time.Minute)
	if name := m.groupName(nil, jid); name != "" {
		t.Fatalf("expired entry served: %q", name)
	}
}

// O WhatsApp entrega a chave de grupo (sender key) como uma parte separada da MESMA
// mensagem: a biblioteca despacha primeiro um evento só com a distribuição da chave e
// depois o conteúdo, os dois com o mesmo id. A parte sem conteúdo não pode ocupar a
// vaga da deduplicação, senão o painel recebe uma bolha vazia e o texto se perde.
func TestGroupSenderKeyPartDoesNotSwallowTheContent(t *testing.T) {
	m, store := panelManager(t)
	keyPart := groupEvent("group-1", false)
	keyPart.Message = &waE2E.Message{SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{GroupID: proto.String(testGroup + "@g.us")}}
	m.onGroupMessage("instance-1", keyPart)
	m.onGroupMessage("instance-1", groupEvent("group-1", false))
	got := queuedMessages(t, m, store)
	if len(got) != 1 || got[0]["text"] != "Alguém usa o DietSystem?" {
		t.Fatalf("expected the group content delivered once, got %v", got)
	}
	// Mensagens de protocolo (revogação, edição) também não viram bolha vazia no painel.
	revoke := groupEvent("group-2", false)
	revoke.Message = &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_REVOKE.Enum()}}
	m.onGroupMessage("instance-1", revoke)
	if got := queuedMessages(t, m, store); len(got) != 1 {
		t.Fatalf("a part without text or media must not reach the panel: %v", got)
	}
	// Uma parte que traz a chave junto com o texto continua chegando normalmente.
	both := groupEvent("group-3", false)
	both.Message.SenderKeyDistributionMessage = &waE2E.SenderKeyDistributionMessage{GroupID: proto.String(testGroup + "@g.us")}
	m.onGroupMessage("instance-1", both)
	if got := queuedMessages(t, m, store); len(got) != 2 || got[1]["text"] != "Alguém usa o DietSystem?" {
		t.Fatalf("content delivered together with the key was lost: %v", got)
	}
}
