package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
)

func (h *Handlers) rotateInstanceToken(w http.ResponseWriter, r *http.Request) {
	if h.cfg.AdminAPIKey == "" {
		writeErr(w, 403, "administrator key required")
		return
	}
	tag := r.Header.Get("If-Match")
	if tag == "" {
		writeErr(w, http.StatusPreconditionRequired, "credential fingerprint required")
		return
	}
	id := r.PathValue("id")
	if handleErr(w, h.mgr.rotateToken(r.Context(), id, tag)) {
		return
	}
	h.mgr.auditInstance(id, logCategorySystem, "instance_token_rotated", "info", InstanceLog{Source: "admin_api"})
	w.WriteHeader(http.StatusNoContent)
}

func (m *Manager) rotateToken(ctx context.Context, id, tag string) error {
	rt := m.get(id)
	if rt == nil {
		return errNotFound
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	current := rt.meta.Token
	if tag != fmt.Sprintf("\"%x\"", sha256.Sum256([]byte(current))) {
		return &apiError{Status: http.StatusPreconditionFailed, Msg: "credential has changed"}
	}
	next := randToken()
	updated := nowRFC()
	result, err := m.store.db.ExecContext(ctx, "UPDATE instances SET token=?,updated_at=? WHERE id=? AND token=?", next, updated, id, current)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return &apiError{Status: http.StatusPreconditionFailed, Msg: "credential has changed"}
	}
	rt.meta.Token = next
	rt.meta.UpdatedAt = updated
	return nil
}
