package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// Regression: a message Thiago types on the phone of a number paired in two
// instances (welcome_bot and nutricionist_46) reaches both sessions with the SAME
// id. De-duplicating the fromMe echo by message id alone let whichever instance
// processed it first win; when nutricionist_46 won, welcome_bot never told the
// SDR that a human took the conversation over and the bot kept answering.
func TestOwnEchoWebhookDeliversToEveryInstanceOfTheSameNumber(t *testing.T) {
	received := make(chan string, 2)
	webhookFor := func(name string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode webhook body: %v", err)
			}
			received <- name
			w.WriteHeader(http.StatusNoContent)
		}))
	}
	sdrServer := webhookFor("welcome_bot")
	defer sdrServer.Close()
	globalServer := webhookFor("nutricionist_46")
	defer globalServer.Close()

	m := testUazapiCompatManager(t, Config{})
	welcome, err := m.Create("welcome_bot", "welcome_bot", sdrServer.URL, "shared-secret")
	if err != nil {
		t.Fatal(err)
	}
	personal, err := m.Create("nutricionist_46", "46", globalServer.URL, "shared-secret")
	if err != nil {
		t.Fatal(err)
	}

	lead := types.NewJID("5567981490781", types.DefaultUserServer)
	owner := types.NewJID("556791234895", types.DefaultUserServer)
	echo := func() *events.Message {
		return &events.Message{
			Info: types.MessageInfo{
				MessageSource: types.MessageSource{Chat: lead, Sender: owner, IsFromMe: true},
				ID:            "same-own-message-id",
				Timestamp:     time.Now(),
			},
			Message: &waE2E.Message{Conversation: proto.String("Oi, aqui é o Thiago")},
		}
	}
	// WhatsApp multi-device hands the same own message to both paired sessions.
	m.onOwnMessage(personal.ID, echo())
	m.onOwnMessage(welcome.ID, echo())

	got := map[string]int{}
	for i := 0; i < 2; i++ {
		select {
		case name := <-received:
			got[name]++
		case <-time.After(2 * time.Second):
			t.Fatalf("only %v received the echo; every instance must deliver its own webhook once", got)
		}
	}
	if got["welcome_bot"] != 1 || got["nutricionist_46"] != 1 {
		t.Fatalf("deliveries = %v, want exactly one per instance", got)
	}

	// The same instance seeing the same echo again is still a duplicate.
	m.onOwnMessage(welcome.ID, echo())
	select {
	case name := <-received:
		t.Fatalf("instance %s delivered the same echo twice", name)
	case <-time.After(300 * time.Millisecond):
	}
}
