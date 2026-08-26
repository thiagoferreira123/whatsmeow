package main

import (
	"testing"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

// O opt-in é o que garante que as instâncias dos nutricionistas NUNCA entram
// na colheita: lista vazia = ninguém; match por nome OU id, case-insensitive.
func TestHistoryHarvesterOptIn(t *testing.T) {
	t.Setenv("HISTORY_HARVEST_INSTANCES", " Welcome_Bot , abc-123 ")
	h := newHistoryHarvester()
	if !h.enabledFor(Instance{Name: "welcome_bot", ID: "x"}) {
		t.Fatal("match por nome (case-insensitive) deveria habilitar")
	}
	if !h.enabledFor(Instance{Name: "outro", ID: "ABC-123"}) {
		t.Fatal("match por id deveria habilitar")
	}
	if h.enabledFor(Instance{Name: "nutricionist_123", ID: "y"}) {
		t.Fatal("instância fora da lista jamais pode entrar na colheita")
	}

	t.Setenv("HISTORY_HARVEST_INSTANCES", "")
	if newHistoryHarvester().enabledFor(Instance{Name: "welcome_bot", ID: "x"}) {
		t.Fatal("lista vazia = colheita desligada para todos")
	}
}

func TestSanitizeHistoryName(t *testing.T) {
	for in, want := range map[string]string{
		"welcome_bot": "welcome_bot",
		"a/b\\c..":    "a_b_c__",
		"":            "instance",
	} {
		if got := sanitizeHistoryName(in); got != want {
			t.Fatalf("sanitize(%q) = %q, esperado %q", in, got, want)
		}
	}
}

func TestHistoryTextExtractsCaptions(t *testing.T) {
	img := &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Caption: proto.String("legenda da foto")}}
	if got := historyText(img); got != "legenda da foto" {
		t.Fatalf("caption de imagem: %q", got)
	}
	conv := &waE2E.Message{Conversation: proto.String("oi")}
	if got := historyText(conv); got != "oi" {
		t.Fatalf("conversation: %q", got)
	}
	if got := historyText(nil); got != "" {
		t.Fatalf("nil deve dar vazio, veio %q", got)
	}
}
