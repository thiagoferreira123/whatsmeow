package main

import (
	"context"
	"errors"
	"testing"

	"go.mau.fi/whatsmeow/store"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func TestSyncWAVersionAppliesLatest(t *testing.T) {
	orig := store.GetWAVersion()
	defer store.SetWAVersion(orig)

	latest := store.WAVersionContainer{orig[0], orig[1], orig[2] + 1}
	got := syncWAVersion(context.Background(), func(context.Context) (*store.WAVersionContainer, error) {
		return &latest, nil
	}, waLog.Noop)

	if got != latest {
		t.Fatalf("syncWAVersion returned %s, want %s", got, latest)
	}
	if store.GetWAVersion() != latest {
		t.Fatalf("store advertises %s, want %s", store.GetWAVersion(), latest)
	}
}

func TestSyncWAVersionKeepsBakedInOnFetchFailure(t *testing.T) {
	orig := store.GetWAVersion()
	defer store.SetWAVersion(orig)

	got := syncWAVersion(context.Background(), func(context.Context) (*store.WAVersionContainer, error) {
		return nil, errors.New("network down")
	}, waLog.Noop)

	if got != orig {
		t.Fatalf("syncWAVersion returned %s, want baked-in %s", got, orig)
	}
	if store.GetWAVersion() != orig {
		t.Fatalf("store advertises %s, want baked-in %s", store.GetWAVersion(), orig)
	}
}

func TestSyncWAVersionNeverDowngrades(t *testing.T) {
	orig := store.GetWAVersion()
	defer store.SetWAVersion(orig)

	older := store.WAVersionContainer{orig[0], orig[1], orig[2] - 1}
	got := syncWAVersion(context.Background(), func(context.Context) (*store.WAVersionContainer, error) {
		return &older, nil
	}, waLog.Noop)

	if got != orig {
		t.Fatalf("syncWAVersion returned %s, want baked-in %s", got, orig)
	}
	if store.GetWAVersion() != orig {
		t.Fatalf("store advertises %s, want baked-in %s", store.GetWAVersion(), orig)
	}
}
