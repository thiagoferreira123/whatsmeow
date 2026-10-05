package main

import (
	"context"
	"net/http"
	"sort"
	"time"

	"go.mau.fi/whatsmeow/types"
)

// Os grupos em que a conta participa, para o painel listá-los antes da primeira
// mensagem chegar. Só id, assunto, criação e tamanho: integrantes não são expostos.
type panelGroup struct {
	JID          string `json:"jid"`
	Name         string `json:"name"`
	CreatedAt    string `json:"createdAt,omitempty"`
	Participants int    `json:"participants"`
}

func panelGroups(groups []*types.GroupInfo) []panelGroup {
	items := make([]panelGroup, 0, len(groups))
	for _, g := range groups {
		if g == nil || g.JID.Server != types.GroupServer {
			continue
		}
		item := panelGroup{JID: g.JID.String(), Name: g.Name, Participants: len(g.Participants)}
		if !g.GroupCreated.IsZero() {
			item.CreatedAt = g.GroupCreated.UTC().Format(time.RFC3339)
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Name != items[j].Name {
			return items[i].Name < items[j].Name
		}
		return items[i].JID < items[j].JID
	})
	return items
}

func (h *Handlers) uzPanelGroups(w http.ResponseWriter, r *http.Request) {
	in, ok := h.panelInstance(w, r)
	if !ok {
		return
	}
	rt, err := h.mgr.requireLoggedIn(in.ID)
	if err != nil {
		writeErr(w, 503, "instance unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	groups, err := rt.client.GetJoinedGroups(ctx)
	if err != nil {
		writeErr(w, 503, "groups unavailable")
		return
	}
	// A listagem já trouxe o assunto de todos: a próxima mensagem não precisa consultar.
	for _, g := range groups {
		if g != nil {
			h.mgr.groupNames.put(g.JID, g.Name, groupNameTTL)
		}
	}
	writeJSON(w, 200, map[string]any{"items": panelGroups(groups)})
}
