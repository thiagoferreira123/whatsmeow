package main

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
	_ "modernc.org/sqlite"
)

func TestLoadAllKeepsHundredsOfInstancesInOneRuntime(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scale-500?mode=memory&cache=shared&_pragma=foreign_keys(on)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = db.Close() })

	container := sqlstore.NewWithDB(db, "sqlite3", waLog.Noop)
	if err := container.Upgrade(context.Background()); err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(db)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		now := nowRFC()
		instance := Instance{
			ID: fmt.Sprintf("instance-%03d", i), Name: fmt.Sprintf("professional-%03d", i),
			Token: fmt.Sprintf("token-%03d", i), CreatedAt: now, UpdatedAt: now,
		}
		if err := store.Create(&instance); err != nil {
			t.Fatalf("create instance %d: %v", i, err)
		}
	}

	mgr := NewManager(container, store, Config{ConnectConcurrency: 8}, waLog.Noop)
	if err := mgr.LoadAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(mgr.runtimes); got != 500 {
		t.Fatalf("loaded runtimes = %d, want 500", got)
	}
}

func TestReconnectBackoffGrowsAndCaps(t *testing.T) {
	// fails=0 → ~30s (24-36s with ±20% jitter)
	for i := 0; i < 20; i++ {
		d := reconnectBackoff(0)
		if d < 24*time.Second || d > 36*time.Second {
			t.Fatalf("backoff(0) = %s, want 24s-36s", d)
		}
	}
	// fails=2 → ~2m (96-144s)
	d := reconnectBackoff(2)
	if d < 96*time.Second || d > 144*time.Second {
		t.Fatalf("backoff(2) = %s, want 96s-144s", d)
	}
	// huge fail counts cap at 10m (8-12m with jitter)
	for _, fails := range []int{6, 10, 63} {
		d := reconnectBackoff(fails)
		if d < 8*time.Minute || d > 12*time.Minute {
			t.Fatalf("backoff(%d) = %s, want 8m-12m (capped)", fails, d)
		}
	}
}

func TestJIDCacheStoreGetAndExpiry(t *testing.T) {
	m := &Manager{jidCache: make(map[string]jidCacheEntry)}
	jid := types.NewJID("5511999998888", types.DefaultUserServer)

	if _, ok := m.cachedJID("k"); ok {
		t.Fatal("empty cache should miss")
	}
	m.storeJID("k", jid)
	got, ok := m.cachedJID("k")
	if !ok || got != jid {
		t.Fatalf("cachedJID = %v/%v, want %v/true", got, ok, jid)
	}
	// Expired entries miss.
	m.jidCache["k"] = jidCacheEntry{jid: jid, exp: time.Now().Add(-time.Second)}
	if _, ok := m.cachedJID("k"); ok {
		t.Fatal("expired entry should miss")
	}
}

func TestJIDCacheEvictsWhenFull(t *testing.T) {
	m := &Manager{jidCache: make(map[string]jidCacheEntry)}
	jid := types.NewJID("5511999998888", types.DefaultUserServer)
	// Fill past the cap with live entries — store must not grow unbounded.
	for i := 0; i < jidCacheMaxSize; i++ {
		m.jidCache[string(rune(i))+"x"] = jidCacheEntry{jid: jid, exp: time.Now().Add(time.Hour)}
	}
	m.storeJID("new", jid)
	if len(m.jidCache) > jidCacheMaxSize+1 {
		t.Fatalf("cache size %d exceeds cap %d", len(m.jidCache), jidCacheMaxSize)
	}
	if _, ok := m.cachedJID("new"); !ok {
		t.Fatal("newly stored key must be present after eviction")
	}
}

func TestDedup(t *testing.T) {
	ws := NewWebhookSender()
	if !ws.dedup("msg1") {
		t.Fatal("first sighting must be new")
	}
	if ws.dedup("msg1") {
		t.Fatal("second sighting must be deduped")
	}
	if !ws.dedup("") {
		t.Fatal("empty id is always new")
	}
	// Old entries are swept once the sweep interval elapses.
	ws.seen["old"] = time.Now().Add(-3 * time.Hour)
	ws.lastSweep = time.Now().Add(-11 * time.Minute)
	ws.dedup("trigger-sweep")
	if _, ok := ws.seen["old"]; ok {
		t.Fatal("entry older than 2h must be swept")
	}
}
