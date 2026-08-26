package main

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// testManagerWithDeviceStore builds a Manager over an in-memory SQLite holding
// BOTH the instances table and whatsmeow's device store — the same single-DB
// layout main.go uses in production.
func testManagerWithDeviceStore(t *testing.T, cfg Config) (*Manager, *instanceRuntime) {
	t.Helper()
	dsn := fmt.Sprintf("file:loggedout-%d?mode=memory&cache=shared&_pragma=foreign_keys(on)", time.Now().UnixNano())
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	container := sqlstore.NewWithDB(db, "sqlite3", nil)
	if err := container.Upgrade(context.Background()); err != nil {
		t.Fatal(err)
	}
	instanceStore, err := NewStore(db)
	if err != nil {
		t.Fatal(err)
	}
	in := Instance{ID: "instance-1", Name: "nutricionist_1", Token: "token", CreatedAt: nowRFC(), UpdatedAt: nowRFC()}
	if err := instanceStore.Create(&in); err != nil {
		t.Fatal(err)
	}
	rt := &instanceRuntime{meta: in}
	m := &Manager{
		runtimes:  map[string]*instanceRuntime{in.ID: rt},
		container: container,
		store:     instanceStore,
		cfg:       cfg,
		outbound:  newOutboundGuard(cfg),
		log:       waLog.Noop,
	}
	m.SetRuntimeActive(true)
	return m, rt
}

// Reproduz o caso do profissional 999815990 (26/08/2026): pareou, o WhatsApp
// devolveu 401 "logged out from another device" um minuto depois e, a partir
// dali, todo POST /instance/connect respondeu 500 "invalid use of deleted
// device" — ou seja, ele nunca mais conseguiu um QR novo.
//
// Causa: no 401 a própria lib apaga o device local (Store.Delete → ID=nil,
// Deleted=true) e QUALQUER Connect() daquele *Client passa a devolver
// ErrDeviceDeleted. Se o runtime continuar segurando esse client, a instância
// fica presa até o processo reiniciar.
func TestLoggedOutRecyclesDeviceSoANewQRCanBeGenerated(t *testing.T) {
	ctx := context.Background()
	m, rt := testManagerWithDeviceStore(t, Config{})

	device := m.container.NewDevice()
	jid := types.NewJID("5531920023640", types.DefaultUserServer)
	device.ID = &jid // pareado
	m.attachClient(rt, device)

	// O que whatsmeow faz sozinho ao receber o 401 (connectionevents.go).
	if err := device.Delete(ctx); err != nil {
		t.Fatal(err)
	}

	m.onLoggedOut("instance-1", &events.LoggedOut{OnConnect: true, Reason: events.ConnectFailureLoggedOut})

	rt.mu.RLock()
	client := rt.client
	rt.mu.RUnlock()
	if client == nil || client.Store == nil {
		t.Fatal("runtime ficou sem client depois do logged_out")
	}
	if client.Store.Deleted {
		t.Fatal("runtime seguiu com o device apagado — Connect()/QR devolvem ErrDeviceDeleted para sempre")
	}
	if client.Store.ID != nil {
		t.Fatalf("device novo deveria estar despareado, ID=%v", client.Store.ID)
	}
	if status := m.statusOf(rt); status != "disconnected" {
		t.Fatalf("status = %q; quero \"disconnected\"", status)
	}
}
