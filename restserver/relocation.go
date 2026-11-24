package main

import "net/http"

// The creation log must still be present: expired audit history cannot establish eligibility.
const unusedInstancePredicate = `status='disconnected' AND jid='' AND owner=''
 AND name='nutricionist_' || admin_field01 AND admin_field01<>'' AND admin_field01 NOT GLOB '*[^0-9]*' AND admin_field01<>'46'
 AND last_disconnect_reason='' AND last_reset_at='' AND webhook_enabled=1 AND webhook_url=? AND webhook_secret=?
 AND webhook_events=? AND webhook_exclude_messages=? AND webhook_add_url_events=0 AND webhook_add_url_types_messages=0
 AND EXISTS (SELECT 1 FROM instance_logs WHERE instance_id=instances.id AND event='instance_created')
 AND NOT EXISTS (SELECT 1 FROM instance_logs WHERE instance_id=instances.id AND event<>'instance_created')
 AND NOT EXISTS (SELECT 1 FROM outbound_queue WHERE instance_id=instances.id)
 AND NOT EXISTS (SELECT 1 FROM outbound_activity WHERE instance_id=instances.id)
 AND NOT EXISTS (SELECT 1 FROM recipient_permissions WHERE instance_id=instances.id)
 AND NOT EXISTS (SELECT 1 FROM recipient_aliases WHERE instance_id=instances.id)`

func (h *Handlers) relocationArgs() []any {
	return []any{h.cfg.UazapiCompatWebhookURL, h.cfg.WebhookSecret, defaultUazapiWebhookEvents, defaultUazapiWebhookExcludeMessages}
}

func (h *Handlers) relocatableInstances(w http.ResponseWriter, r *http.Request) {
	rows, err := h.mgr.store.db.QueryContext(r.Context(), "SELECT id FROM instances WHERE "+unusedInstancePredicate, h.relocationArgs()...)
	if handleErr(w, err) {
		return
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if handleErr(w, rows.Scan(&id)) {
			return
		}
		ids = append(ids, id)
	}
	if !handleErr(w, rows.Err()) {
		writeJSON(w, 200, ids)
	}
}

// Never call Delete here: it is allowed to log out a live WhatsApp.
func (h *Handlers) deleteUnusedInstance(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rt := h.mgr.get(id)
	if rt == nil {
		handleErr(w, errNotFound)
		return
	}
	rt.mu.Lock()
	if rt.qrRequested || rt.qrRunning || rt.resetting || rt.meta.JID != "" {
		rt.mu.Unlock()
		writeErr(w, 409, "instance in use")
		return
	}
	result, err := h.mgr.store.db.ExecContext(r.Context(), "DELETE FROM instances WHERE id=? AND "+unusedInstancePredicate, append([]any{id}, h.relocationArgs()...)...)
	if err != nil {
		rt.mu.Unlock()
		handleErr(w, err)
		return
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		rt.mu.Unlock()
		writeErr(w, 409, "instance in use")
		return
	}
	rt.relocated = true
	rt.mu.Unlock()
	h.mgr.mu.Lock()
	delete(h.mgr.runtimes, id)
	h.mgr.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}
