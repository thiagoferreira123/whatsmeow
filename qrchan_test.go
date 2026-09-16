package whatsmeow

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func TestQRChannelContextCancellationClosesBeforeFirstCode(t *testing.T) {
	client := NewClient(&store.Device{}, waLog.Noop)
	ctx, cancel := context.WithCancel(context.Background())
	qrChannel, err := client.GetQRChannel(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cancel()

	select {
	case _, open := <-qrChannel:
		if open {
			t.Fatal("QR channel emitted an item after cancellation; want it closed")
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("QR channel stayed open after context cancellation before the first QR event")
	}

	deadline := time.Now().Add(250 * time.Millisecond)
	for {
		client.eventHandlersLock.RLock()
		handlers := len(client.eventHandlers)
		client.eventHandlersLock.RUnlock()
		if handlers == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("QR event handler remained registered after cancellation: handlers=%d", handlers)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// O WhatsApp passou a mandar companion_reg_refresh (rotação do segredo ADV) no
// meio do pareamento. Cliente que não trata esse evento — ou que trata e cai no
// fluxo de fechamento (tulir/whatsmeow#1267) — fecha o canal com um item vazio e
// o pareamento morre calado: o celular mostra "Verifique sua conexão e tente
// novamente". O canal tem que seguir vivo e reemitir o código com o segredo novo.
func TestQRChannelRotateADVSecretKeepsChannelOpen(t *testing.T) {
	client := NewClient(&store.Device{}, waLog.Noop)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	qrChannel, err := client.GetQRChannel(ctx)
	if err != nil {
		t.Fatal(err)
	}

	client.dispatchEvent(&events.QR{Codes: []string{
		"ref1,key1,SEGREDO-VELHO,adv1",
		"ref2,key2,SEGREDO-VELHO,adv2",
	}})

	first := receiveQRItem(t, qrChannel)
	if first.Event != QRChannelEventCode || !strings.Contains(first.Code, "SEGREDO-VELHO") {
		t.Fatalf("primeiro item = %+v; queria um code contendo SEGREDO-VELHO", first)
	}

	client.dispatchEvent(&events.RotateADVSecret{OldSecret: "SEGREDO-VELHO", NewSecret: "SEGREDO-NOVO"})

	next := receiveQRItem(t, qrChannel)
	if next.Event != QRChannelEventCode {
		t.Fatalf("após RotateADVSecret o canal entregou %+v; queria outro code", next)
	}
	if !strings.Contains(next.Code, "SEGREDO-NOVO") {
		t.Fatalf("código não rotacionado após RotateADVSecret: %q", next.Code)
	}
}

func receiveQRItem(t *testing.T, ch <-chan QRChannelItem) QRChannelItem {
	t.Helper()
	select {
	case item, open := <-ch:
		if !open {
			t.Fatal("canal de QR fechou no meio do pareamento")
		}
		return item
	case <-time.After(2 * time.Second):
		t.Fatal("nenhum item chegou no canal de QR")
		return QRChannelItem{}
	}
}

func TestQRChannelCancellationCanRaceWithTerminalEvent(t *testing.T) {
	for range 100 {
		client := NewClient(&store.Device{}, waLog.Noop)
		ctx, cancel := context.WithCancel(context.Background())
		qrChannel, err := client.GetQRChannel(ctx)
		if err != nil {
			t.Fatal(err)
		}
		dispatched := make(chan struct{})
		go func() {
			client.dispatchEvent(&events.PairSuccess{})
			close(dispatched)
		}()

		cancel()
		for range qrChannel {
		}
		<-dispatched
	}
}
