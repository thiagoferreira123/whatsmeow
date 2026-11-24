package main

import (
	"crypto/subtle"
	"net/http"
)

// Provisioning clients manage the instances assigned to their server-side owner.
// Their credential never enters the global administrator router.
func (h *Handlers) withProvisioning(fallback http.Handler) http.Handler {
	type route struct {
		key     string
		handler http.Handler
	}
	routes := make([]route, 0, len(h.cfg.ProvisioningKeys))
	for owner, key := range h.cfg.ProvisioningKeys {
		mux := http.NewServeMux()
		create := func(compat bool) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Name  string `json:"name"`
					Owner string `json:"adminField01"`
				}
				if !readJSON(w, r, &body) {
					return
				}
				if body.Owner != owner || body.Name == "" {
					writeErr(w, 403, "invalid provisioning owner or name")
					return
				}
				var webhookURL, webhookSecret string
				if compat {
					webhookURL, webhookSecret = h.cfg.UazapiCompatWebhookURL, h.cfg.WebhookSecret
				}
				in, err := h.mgr.CreateIdempotent(r.Header.Get("Idempotency-Key"), body.Name, owner, webhookURL, webhookSecret)
				if handleErr(w, err) {
					return
				}
				if compat {
					writeJSON(w, 200, map[string]any{"instance": h.uzInstanceObj(in), "token": in.Token})
					return
				}
				writeJSON(w, 201, h.uzInstanceObj(in))
			}
		}
		mux.HandleFunc("POST /instances", create(false))
		mux.HandleFunc("POST /instance/init", create(true))
		mux.HandleFunc("GET /instance/all", func(w http.ResponseWriter, r *http.Request) {
			list, err := h.mgr.List()
			if handleErr(w, err) {
				return
			}
			owned := make([]map[string]any, 0)
			for _, in := range list {
				if in.AdminField01 == owner {
					owned = append(owned, h.uzInstanceObj(in))
				}
			}
			writeJSON(w, 200, owned)
		})
		bind := func(pattern string, handler http.HandlerFunc) {
			mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
				in, err := h.mgr.Get(r.PathValue("id"))
				if err != nil || in.AdminField01 != owner {
					writeErr(w, 404, "instance not found")
					return
				}
				handler(w, r)
			})
		}
		bind("GET /instances/{id}/contact", h.getContact)
		bind("GET /instances/{id}/status", h.getStatus)
		bind("GET /instances/{id}/qr", h.getQR)
		bind("POST /instances/{id}/send/text", h.sendText)
		routes = append(routes, route{key, h.withRuntimeOwner(mux)})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("admintoken")
		if key == "" {
			key = h.providedKey(r)
		}
		for _, entry := range routes {
			if subtle.ConstantTimeCompare([]byte(key), []byte(entry.key)) == 1 {
				entry.handler.ServeHTTP(w, r)
				return
			}
		}
		fallback.ServeHTTP(w, r)
	})
}
