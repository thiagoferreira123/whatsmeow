package main

import (
	_ "embed"
	"net/http"
)

//go:embed static/index.html
var indexHTML []byte

// serveUI returns the embedded single-page management UI.
func (h *Handlers) serveUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}
