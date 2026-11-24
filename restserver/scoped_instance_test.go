package main

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestScopedInstanceOperations(t *testing.T) {
	cfg := Config{AdminAPIKey: "admin-fixture"}
	manager := testUazapiCompatManager(t, cfg)
	first, err := manager.Create("test-a", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.Create("test-b", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	router := NewHandlers(manager, cfg).Router()
	call := func(method, path, token, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("token", token)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	for _, op := range []struct{ method, path string }{
		{"GET", "/status"}, {"GET", "/qr.png"}, {"POST", "/resume"},
		{"POST", "/send/text"}, {"POST", "/send/media"},
		{"GET", "/consents/15555550100"}, {"POST", "/consents"},
	} {
		if r := call(op.method, "/instance/self"+op.path, "", ""); r.Code != 401 {
			t.Errorf("unauthenticated %s: got %d", op.path, r.Code)
		}
	}
	if r := call("GET", "/instance/self/status", first.Token, ""); r.Code != 200 {
		t.Fatalf("scoped status: %d", r.Code)
	}
	if r := call("POST", "/instance/self/consents", first.Token, `{"number":"15555550100","source":"test"}`); r.Code != 200 {
		t.Fatalf("grant: %d", r.Code)
	}
	for _, check := range []struct {
		token   string
		granted bool
	}{{first.Token, true}, {second.Token, false}} {
		r := call("GET", "/instance/self/consents/15555550100", check.token, "")
		if r.Code != 200 || strings.Contains(r.Body.String(), `"granted"`) != check.granted {
			t.Errorf("wrong instance consent: %d", r.Code)
		}
	}
	if r := call("GET", "/instances", first.Token, ""); r.Code != 401 {
		t.Errorf("global list: %d", r.Code)
	}
}
