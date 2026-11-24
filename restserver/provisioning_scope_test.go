package main

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProvisioningCredentialsAreBoundToOwner(t *testing.T) {
	t.Setenv("PROVISIONING_KEYS_JSON", `{"fixture-a":"delegated-fixture"}`)
	t.Setenv("ADMIN_API_KEY", "global-fixture")
	cfg := loadConfig()
	cfg.AdminAPIKey = "global-fixture"
	m := testUazapiCompatManager(t, cfg)
	a, err := m.Create("first", "fixture-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	b, err := m.Create("second", "fixture-b", "", "")
	if err != nil {
		t.Fatal(err)
	}
	router := NewHandlers(m, cfg).Router()
	call := func(method, path, body, header string) (int, string) {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		if header == "Authorization" {
			r.Header.Set(header, "Bearer delegated-fixture")
		} else {
			r.Header.Set(header, "delegated-fixture")
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		return w.Code, w.Body.String()
	}
	status, body := call("GET", "/instance/all", "", "admintoken")
	if status != 200 || !strings.Contains(body, a.ID) || strings.Contains(body, b.ID) {
		t.Fatalf("owner listing: %d", status)
	}
	for _, path := range []string{"/instances/" + b.ID + "/status", "/instances", "/webhook/config"} {
		code, _ := call("GET", path, "", "Authorization")
		if code < 400 {
			t.Errorf("out of scope: %d", code)
		}
	}
	code, _ := call("GET", "/instances/"+a.ID+"/status", "", "Authorization")
	if code != 200 {
		t.Errorf("own status: %d", code)
	}
	for _, check := range []struct{ path, header string }{{"/instances", "Authorization"}, {"/instance/init", "admintoken"}} {
		code, _ = call("POST", check.path, `{"name":"wrong","adminField01":"fixture-b"}`, check.header)
		if code < 400 {
			t.Errorf("foreign create: %d", code)
		}
		code, body = call("POST", check.path, `{"name":"new","adminField01":"fixture-a"}`, check.header)
		if code < 200 || code >= 300 || !strings.Contains(body, "id") {
			t.Errorf("owned create: %d", code)
		}
	}
}
