package whatsmeow

import (
	"context"
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
