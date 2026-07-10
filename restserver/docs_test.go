package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDocumentationRoutesArePublicAndComplete(t *testing.T) {
	m, _ := testPolicyManager(t, Config{})
	router := NewHandlers(m, Config{AdminAPIKey: "secret"}).Router()

	docsRec := httptest.NewRecorder()
	router.ServeHTTP(docsRec, httptest.NewRequest(http.MethodGet, "/docs", nil))
	if docsRec.Code != http.StatusOK {
		t.Fatalf("docs status=%d body=%s", docsRec.Code, docsRec.Body.String())
	}
	docs := docsRec.Body.String()
	for _, required := range []string{
		"Developer Docs", "Autenticação", "Primeira chamada", "Erros, filas e limites",
		"/instances/{id}/send/text", "/instances/{id}/logs", "/instance/init", "/send/media",
	} {
		if !strings.Contains(docs, required) {
			t.Fatalf("documentation missing %q", required)
		}
	}

	specRec := httptest.NewRecorder()
	router.ServeHTTP(specRec, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	if specRec.Code != http.StatusOK {
		t.Fatalf("openapi status=%d body=%s", specRec.Code, specRec.Body.String())
	}
	var spec struct {
		OpenAPI string                     `json:"openapi"`
		Paths   map[string]json.RawMessage `json:"paths"`
	}
	if err := json.Unmarshal(specRec.Body.Bytes(), &spec); err != nil {
		t.Fatalf("invalid OpenAPI JSON: %v", err)
	}
	if spec.OpenAPI != "3.1.0" {
		t.Fatalf("openapi version=%q", spec.OpenAPI)
	}
	expectedPaths := []string{
		"/health", "/live", "/metrics", "/webhook/config", "/webhook", "/instances", "/instances/{id}",
		"/instances/{id}/qr", "/instances/{id}/qr.png", "/instances/{id}/status",
		"/instances/{id}/profile", "/instances/{id}/contact", "/instances/{id}/send/text",
		"/instances/{id}/send/media", "/instances/{id}/queue", "/instances/{id}/logs",
		"/instances/{id}/webhook", "/instances/{id}/consents/{number}", "/instances/{id}/consents",
		"/instances/{id}/consents/revoke", "/instances/{id}/disconnect", "/instances/{id}/hibernate",
		"/instances/{id}/resume", "/instances/{id}/reset", "/instance/init", "/instance/connect",
		"/instance/status", "/instance/all", "/instance/disconnect", "/instance/hibernate",
		"/instance/resume", "/instance/reset", "/instance", "/send/text", "/send/media", "/message/async",
	}
	for _, path := range expectedPaths {
		if _, ok := spec.Paths[path]; !ok {
			t.Errorf("OpenAPI missing path %s", path)
		}
		if !strings.Contains(docs, path) {
			t.Errorf("navigable documentation missing path %s", path)
		}
	}
	if !strings.Contains(string(indexHTML), `href="/docs"`) {
		t.Fatal("management navigation missing documentation shortcut")
	}
}
