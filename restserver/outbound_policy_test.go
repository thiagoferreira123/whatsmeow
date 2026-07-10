package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func testPolicyManager(t *testing.T, cfg Config) (*Manager, *Store) {
	t.Helper()
	dsn := fmt.Sprintf("file:policy-%d?mode=memory&cache=shared&_pragma=foreign_keys(on)", time.Now().UnixNano())
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	store, err := NewStore(db)
	if err != nil {
		t.Fatal(err)
	}
	in := Instance{ID: "instance-1", Name: "test", Token: "token", CreatedAt: nowRFC(), UpdatedAt: nowRFC()}
	if err := store.Create(&in); err != nil {
		t.Fatal(err)
	}
	m := &Manager{
		runtimes: map[string]*instanceRuntime{"instance-1": {meta: in}},
		store:    store,
		cfg:      cfg,
		outbound: newOutboundGuard(cfg),
	}
	return m, store
}

func apiStatus(err error) int {
	var ae *apiError
	if errors.As(err, &ae) {
		return ae.Status
	}
	return 0
}

func TestOutboundRequiresConsentAndHonorsRevocation(t *testing.T) {
	cfg := Config{RequireLocalConsent: true, ServiceWindowHours: 24}
	m, _ := testPolicyManager(t, cfg)
	ctx := context.Background()

	if got := apiStatus(m.checkOutbound(ctx, "instance-1", "5565999999999")); got != 403 {
		t.Fatalf("unknown recipient status = %d, want 403", got)
	}
	if _, err := m.GrantConsent("instance-1", "+55 65 99999-9999", "checkout"); err != nil {
		t.Fatal(err)
	}
	if err := m.checkOutbound(ctx, "instance-1", "5565999999999"); err != nil {
		t.Fatalf("granted recipient blocked: %v", err)
	}
	if _, err := m.RevokeConsent("instance-1", "5565999999999", "user_request"); err != nil {
		t.Fatal(err)
	}
	if got := apiStatus(m.checkOutbound(ctx, "instance-1", "5565999999999")); got != 403 {
		t.Fatalf("revoked recipient status = %d, want 403", got)
	}
}

func TestInboundOpensServiceWindowWithoutChangingRevocation(t *testing.T) {
	cfg := Config{RequireLocalConsent: true, ServiceWindowHours: 24}
	m, store := testPolicyManager(t, cfg)
	ctx := context.Background()
	recipient := "5565988887777"

	if err := store.RecordInbound("instance-1", recipient, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := m.checkOutbound(ctx, "instance-1", recipient); err != nil {
		t.Fatalf("recent inbound should open service window: %v", err)
	}
	if _, err := m.RevokeConsent("instance-1", recipient, "inbound_opt_out"); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordInbound("instance-1", recipient, time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := apiStatus(m.checkOutbound(ctx, "instance-1", recipient)); got != 403 {
		t.Fatalf("inbound must not clear explicit revocation; status = %d", got)
	}
}

func TestOutboundGuardCadenceAndRetryAfter(t *testing.T) {
	g := newOutboundGuard(Config{SendRatePerMinute: 60, SendBurst: 1, RecipientCooldown: 10})
	now := time.Now()
	if wait := g.allow("one", "a", now); wait != 0 {
		t.Fatalf("first message wait = %s", wait)
	}
	if wait := g.allow("one", "a", now.Add(time.Second)); wait < 8*time.Second {
		t.Fatalf("recipient cooldown wait = %s, want about 9s", wait)
	}
	if wait := g.allow("one", "b", now); wait < 900*time.Millisecond {
		t.Fatalf("instance token wait = %s, want about 1s", wait)
	}
}

func TestOutboundRollingDailyLimitIsPersistent(t *testing.T) {
	cfg := Config{RecipientDailyMax: 2}
	m, store := testPolicyManager(t, cfg)
	now := time.Now()
	for i := 0; i < 2; i++ {
		if err := store.RecordOutbound("instance-1", "5565991112222", now.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	err := m.checkOutbound(context.Background(), "instance-1", "5565991112222")
	if got := apiStatus(err); got != 429 {
		t.Fatalf("daily-limit status = %d (%v), want 429", got, err)
	}
	var ae *apiError
	if !errors.As(err, &ae) || ae.RetryAfter <= 0 {
		t.Fatalf("daily-limit error missing Retry-After: %#v", err)
	}
}

func TestTemporaryBanCircuitBreakerPersistsOnInstance(t *testing.T) {
	m, store := testPolicyManager(t, Config{})
	rt := m.get("instance-1")
	rt.mu.Lock()
	rt.meta.SendingBlockedUntil = time.Now().Add(15 * time.Minute).UTC().Format(time.RFC3339)
	in := rt.meta
	rt.mu.Unlock()
	if err := store.Save(&in); err != nil {
		t.Fatal(err)
	}

	err := m.checkOutbound(context.Background(), "instance-1", "5565991112222")
	if got := apiStatus(err); got != 429 {
		t.Fatalf("circuit-breaker status = %d (%v), want 429", got, err)
	}
	loaded, err := store.Get("instance-1")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.SendingBlockedUntil == "" {
		t.Fatal("sending block must survive a process restart")
	}
}

func TestConsentStoresBrazilianNinthDigitVariants(t *testing.T) {
	m, store := testPolicyManager(t, Config{})
	if _, err := m.GrantConsent("instance-1", "5565999998888", "checkout"); err != nil {
		t.Fatal(err)
	}
	if p, err := store.GetRecipientPermission("instance-1", "556599998888"); err != nil || p.Status != "granted" {
		t.Fatalf("variant consent = %#v, %v", p, err)
	}
}
