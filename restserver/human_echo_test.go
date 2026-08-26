package main

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
)

// O registro de IDs enviados pela API é o que separa "eco do próprio bot"
// (wasSentByApi=true, descartado no n8n) de "resposta humana manual"
// (wasSentByApi=false, ativa o human_lock). Falso negativo silencia o bot
// (aceitável); falso positivo faria o bot atropelar conversa humana (nunca).
func TestSentEchoRegistry(t *testing.T) {
	m := &Manager{sentEchoIDs: make(map[string]time.Time)}

	if m.wasSentByAPI("3EB0AAAA") {
		t.Fatal("id nunca registrado não pode constar como enviado pela API")
	}
	m.recordSentEchoID("3EB0AAAA")
	if !m.wasSentByAPI("3EB0AAAA") {
		t.Fatal("id registrado deve constar como enviado pela API")
	}
	if m.wasSentByAPI("3EB0BBBB") {
		t.Fatal("id diferente não pode constar")
	}

	m.recordSentEchoID("")
	if m.wasSentByAPI("") {
		t.Fatal("id vazio nunca é registrado")
	}

	// expirado: mais velho que o TTL não conta mais como API
	m.sentEchoIDs["3EB0CCCC"] = time.Now().Add(-sentEchoTTL - time.Minute)
	if m.wasSentByAPI("3EB0CCCC") {
		t.Fatal("id expirado não pode constar como enviado pela API")
	}
}

func TestSentEchoRegistryPrune(t *testing.T) {
	m := &Manager{sentEchoIDs: make(map[string]time.Time)}
	old := time.Now().Add(-sentEchoTTL - time.Minute)
	for i := 0; i < 5000; i++ {
		m.sentEchoIDs[string(rune('a'+i%26))+time.Duration(i).String()] = old
	}
	m.recordSentEchoID("fresh")
	if len(m.sentEchoIDs) > 4096 {
		t.Fatalf("prune não rodou: %d entradas", len(m.sentEchoIDs))
	}
	if !m.wasSentByAPI("fresh") {
		t.Fatal("entrada recém-registrada sobreviveu ao prune")
	}
}

func TestResolveOwnChatPhoneAddressed(t *testing.T) {
	info := types.MessageInfo{MessageSource: types.MessageSource{
		Chat: types.NewJID("5521970787757", types.DefaultUserServer),
	}}
	pn, lid := resolveOwnChat(info)
	if pn != "5521970787757@s.whatsapp.net" || lid != "" {
		t.Fatalf("endereçamento por PN incorreto: pn=%q lid=%q", pn, lid)
	}
}

func TestResolveOwnChatLIDUsesRecipientAlt(t *testing.T) {
	info := types.MessageInfo{MessageSource: types.MessageSource{
		Chat:         types.NewJID("20293754081489", types.HiddenUserServer),
		RecipientAlt: types.NewJID("5521970787757", types.DefaultUserServer),
		IsFromMe:     true,
	}}
	pn, lid := resolveOwnChat(info)
	if pn != "5521970787757@s.whatsapp.net" {
		t.Fatalf("RecipientAlt deveria resolver o telefone, recebeu %q", pn)
	}
	if lid != "20293754081489@lid" {
		t.Fatalf("LID da conversa deveria ser preservado, recebeu %q", lid)
	}
}

func TestResolveOwnChatLIDWithoutAltIsExplicitlyUnresolved(t *testing.T) {
	info := types.MessageInfo{MessageSource: types.MessageSource{
		Chat:     types.NewJID("20293754081489", types.HiddenUserServer),
		IsFromMe: true,
	}}
	pn, lid := resolveOwnChat(info)
	if pn != "" || lid != "20293754081489@lid" {
		t.Fatalf("fallback ao mapa deve receber PN vazio e LID preservado: pn=%q lid=%q", pn, lid)
	}
}
