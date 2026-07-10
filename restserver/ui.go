package main

import (
	_ "embed"
	"net/http"
)

//go:embed static/index.html
var indexHTML []byte

//go:embed static/docs.html
var docsHTML []byte

//go:embed static/openapi.json
var openAPIJSON []byte

// serveUI returns the embedded single-page management UI.
func (h *Handlers) serveUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}

func (h *Handlers) serveDocs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(docsHTML)
}

func (h *Handlers) serveOpenAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", `inline; filename="whatsmeow-openapi.json"`)
	_, _ = w.Write(openAPIJSON)
}
