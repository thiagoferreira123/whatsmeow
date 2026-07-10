package main

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestRuntimeLeaseAllowsOnlyOneOwnerAndHandsOff(t *testing.T) {
	db, err := sql.Open("sqlite", "file:runtime-lease?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	first, err := newRuntimeLease(db, "owner-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newRuntimeLease(db, "owner-b", time.Second)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	acquired, err := first.TryAcquire(ctx)
	if err != nil || !acquired {
		t.Fatalf("first acquire = %v, %v; want true, nil", acquired, err)
	}
	acquired, err = second.TryAcquire(ctx)
	if err != nil || acquired {
		t.Fatalf("second acquire = %v, %v; want false, nil", acquired, err)
	}
	renewed, err := first.Renew(ctx)
	if err != nil || renewed {
		t.Fatalf("renew after takeover request = %v, %v; want false, nil", renewed, err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
	acquired, err = second.TryAcquire(ctx)
	if err != nil || !acquired {
		t.Fatalf("handoff acquire = %v, %v; want true, nil", acquired, err)
	}
}

func TestStandbyRuntimeIsLiveButNotReadyAndRejectsTraffic(t *testing.T) {
	mgr, _ := testPolicyManager(t, Config{})
	mgr.SetRuntimeActive(false)
	handler := NewHandlers(mgr, Config{}).Router()

	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	if health.Code != http.StatusServiceUnavailable {
		t.Fatalf("standby health status = %d, want 503", health.Code)
	}

	live := httptest.NewRecorder()
	handler.ServeHTTP(live, httptest.NewRequest(http.MethodGet, "/live", nil))
	if live.Code != http.StatusOK {
		t.Fatalf("liveness status = %d, want 200", live.Code)
	}

	instances := httptest.NewRecorder()
	handler.ServeHTTP(instances, httptest.NewRequest(http.MethodGet, "/instances", nil))
	if instances.Code != http.StatusServiceUnavailable {
		t.Fatalf("standby API status = %d, want 503", instances.Code)
	}

	mgr.SetRuntimeActive(true)
	health = httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("active health status = %d, want 200", health.Code)
	}
	if body := health.Body.String(); !strings.Contains(body, `"version":"runtime-lease-v1"`) {
		t.Fatalf("active health version = %s", body)
	}
	instances = httptest.NewRecorder()
	handler.ServeHTTP(instances, httptest.NewRequest(http.MethodGet, "/instances", nil))
	if instances.Code != http.StatusOK {
		t.Fatalf("active API status = %d, want 200", instances.Code)
	}
}
