package main

import (
	"testing"
	"time"
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
