package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// armPairingLoop finge um pareamento em voo, para provar que quem troca o
// client ou mata a instância também mata o loop antigo.
func armPairingLoop(rt *instanceRuntime) (context.Context, uint64) {
	ctx, cancel := context.WithCancel(context.Background())
	rt.mu.Lock()
	rt.qrRunning = true
	rt.qrCode = "codigo-antigo"
	rt.qrExpiresAt = time.Now().Add(time.Minute)
	rt.qrStartedAt = time.Now()
	rt.qrCancel = cancel
	attempt := rt.qrAttempt
	rt.mu.Unlock()
	return ctx, attempt
}

func assertPairingLoopKilled(t *testing.T, rt *instanceRuntime, ctx context.Context, before uint64) {
	t.Helper()
	rt.mu.RLock()
	running, code, cancel, attempt := rt.qrRunning, rt.qrCode, rt.qrCancel, rt.qrAttempt
	rt.mu.RUnlock()
	if running || code != "" || cancel != nil {
		t.Fatalf("loop antigo sobreviveu: running=%v code=%q cancelSet=%v", running, code, cancel != nil)
	}
	if attempt == before {
		t.Fatal("qrAttempt não foi incrementado — o consumidor antigo continua válido")
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("o contexto do pareamento antigo não foi cancelado")
	}
}

// Trocar o client sem descartar o anterior deixava o velho vivo despachando
// eventos no MESMO handler da instância, e o loop de QR órfão fazia todo pedido
// seguinte esperar por um código que nunca chegava.
func TestAttachClientDisposesPreviousClientAndQRLoop(t *testing.T) {
	m, rt := testManagerWithDeviceStore(t, Config{})
	m.attachClient(rt, m.container.NewDevice())
	rt.mu.RLock()
	previous := rt.client
	rt.mu.RUnlock()

	ctx, before := armPairingLoop(rt)
	m.attachClient(rt, m.container.NewDevice())

	assertPairingLoopKilled(t, rt, ctx, before)
	rt.mu.RLock()
	current := rt.client
	rt.mu.RUnlock()
	if current == previous {
		t.Fatal("o client não foi trocado")
	}
}

// InitialAutoReconnect engole o erro de dial e devolve nil sem socket; para um
// device despareado o autoReconnect é no-op (client.go:606-609), então a flag só
// mascarava a falha do pareamento.
func TestAttachClientEnablesInitialAutoReconnectOnlyForPairedDevice(t *testing.T) {
	m, rt := testManagerWithDeviceStore(t, Config{})

	m.attachClient(rt, m.container.NewDevice())
	rt.mu.RLock()
	unpaired := rt.client.InitialAutoReconnect
	rt.mu.RUnlock()
	if unpaired {
		t.Fatal("device virgem não deve ter InitialAutoReconnect (esconde erro de dial no pareamento)")
	}

	device := m.container.NewDevice()
	jid := types.NewJID("5511999999999", types.DefaultUserServer)
	device.ID = &jid
	m.attachClient(rt, device)
	rt.mu.RLock()
	paired := rt.client.InitialAutoReconnect
	rt.mu.RUnlock()
	if !paired {
		t.Fatal("device pareado precisa manter InitialAutoReconnect (reconexão de boot)")
	}
}

func TestInvalidateQRKillsLoopOnSessionLifecycleOperations(t *testing.T) {
	t.Run("logout", func(t *testing.T) {
		m, rt := testManagerWithDeviceStore(t, Config{})
		m.attachClient(rt, m.container.NewDevice())
		ctx, before := armPairingLoop(rt)
		if _, err := m.Logout(context.Background(), "instance-1"); err != nil {
			t.Fatal(err)
		}
		assertPairingLoopKilled(t, rt, ctx, before)
	})

	t.Run("disconnect", func(t *testing.T) {
		m, rt := testManagerWithDeviceStore(t, Config{})
		m.attachClient(rt, m.container.NewDevice())
		ctx, before := armPairingLoop(rt)
		if err := m.Disconnect("instance-1"); err != nil {
			t.Fatal(err)
		}
		assertPairingLoopKilled(t, rt, ctx, before)
	})

	t.Run("reset runtime", func(t *testing.T) {
		m, rt := testManagerWithDeviceStore(t, Config{})
		device := m.container.NewDevice()
		jid := types.NewJID("5511999999999", types.DefaultUserServer)
		device.ID = &jid
		m.attachClient(rt, device)
		ctx, before := armPairingLoop(rt)
		if _, err := m.ResetRuntime("instance-1"); err != nil {
			t.Fatal(err)
		}
		assertPairingLoopKilled(t, rt, ctx, before)
	})

	t.Run("delete", func(t *testing.T) {
		m, rt := testManagerWithDeviceStore(t, Config{})
		m.attachClient(rt, m.container.NewDevice())
		ctx, before := armPairingLoop(rt)
		if err := m.Delete(context.Background(), "instance-1"); err != nil {
			t.Fatal(err)
		}
		assertPairingLoopKilled(t, rt, ctx, before)
	})
}

// Logout com runtime sem client não pode derrubar o processo.
func TestLogoutSurvivesRuntimeWithoutClient(t *testing.T) {
	m, rt := testManagerWithDeviceStore(t, Config{})
	rt.mu.Lock()
	rt.client = nil
	rt.mu.Unlock()

	if _, err := m.Logout(context.Background(), "instance-1"); err != nil {
		t.Fatal(err)
	}
	rt.mu.RLock()
	cli := rt.client
	rt.mu.RUnlock()
	if cli == nil || cli.Store == nil || cli.Store.Deleted {
		t.Fatal("Logout deveria deixar um device virgem pronto para parear")
	}
}

// pair.go apaga o device local num PairError tardio SEM emitir LoggedOut, então
// a instância ficava com o mesmo sintoma do 401 e ninguém reciclava o device.
func TestOnPairErrorRecyclesDevice(t *testing.T) {
	ctx := context.Background()
	m, rt := testManagerWithDeviceStore(t, Config{})
	device := m.container.NewDevice()
	jid := types.NewJID("5511999999999", types.DefaultUserServer)
	device.ID = &jid
	m.attachClient(rt, device)
	if err := device.Delete(ctx); err != nil {
		t.Fatal(err)
	}

	m.onPairError("instance-1", &events.PairError{ID: jid, Error: errors.New("pair failed")})

	rt.mu.RLock()
	cli := rt.client
	rt.mu.RUnlock()
	if cli == nil || cli.Store == nil {
		t.Fatal("runtime ficou sem client depois do PairError")
	}
	if cli.Store.Deleted {
		t.Fatal("runtime seguiu com o device apagado — Connect()/QR devolveriam ErrDeviceDeleted")
	}
	if cli.Store.ID != nil {
		t.Fatalf("device novo deveria estar despareado, ID=%v", cli.Store.ID)
	}
}
