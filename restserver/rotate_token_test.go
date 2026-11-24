package main

import (
	"crypto/sha256"
	"fmt"
	"net/http/httptest"
	"testing"
)

func TestRotateOneInstanceToken(t *testing.T) {
	cfg := Config{AdminAPIKey: "admin-fixture"}
	m := testUazapiCompatManager(t, cfg)
	first, err := m.Create("first", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := m.Create("second", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandlers(m, cfg).Router()
	call := func(key, precondition string) int {
		r := httptest.NewRequest("POST", "/instances/"+first.ID+"/rotate-token", nil)
		r.Header.Set("Authorization", "Bearer "+key)
		r.Header.Set("If-Match", precondition)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w.Code
	}
	tag := fmt.Sprintf("\"%x\"", sha256.Sum256([]byte(first.Token)))
	if got := call(first.Token, tag); got != 401 {
		t.Fatalf("instance token rotated credential: %d", got)
	}
	if got := call(cfg.AdminAPIKey, ""); got != 428 {
		t.Errorf("missing precondition: %d", got)
	}
	if got := call(cfg.AdminAPIKey, tag); got != 204 {
		t.Fatalf("rotate: %d", got)
	}
	updated, err := m.Get(first.ID)
	if err != nil || updated.Token == first.Token {
		t.Fatal("runtime token not replaced")
	}
	if got := call(cfg.AdminAPIKey, tag); got != 412 {
		t.Fatalf("stale retry: %d", got)
	}
	if err = m.store.Save(&first); err != nil {
		t.Fatal(err)
	}
	if _, err = m.store.GetByToken(first.Token); err == nil {
		t.Fatal("stale save resurrected the revoked token")
	}
	for _, in := range []Instance{updated, second} {
		got, err := m.store.GetByToken(in.Token)
		if err != nil || got.ID != in.ID {
			t.Fatal("token owner changed")
		}
	}
}
