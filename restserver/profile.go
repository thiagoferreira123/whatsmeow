package main

import (
	"context"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

// fetchProfilePic returns the profile picture URL for a JID, or "" if none/private.
func (m *Manager) fetchProfilePic(ctx context.Context, cli *whatsmeow.Client, jid types.JID) string {
	info, err := cli.GetProfilePictureInfo(ctx, jid, &whatsmeow.GetProfilePictureParams{Preview: false})
	if err != nil || info == nil {
		return ""
	}
	return info.URL
}

// captureOwnerProfile fills name / phone / photo / isBusiness for the connected
// account and persists it. Safe to call from a goroutine after connect.
func (m *Manager) captureOwnerProfile(ctx context.Context, id string) {
	rt := m.get(id)
	if rt == nil {
		return
	}
	cli := rt.client
	if cli == nil || cli.Store == nil || cli.Store.ID == nil {
		return
	}
	jid := cli.Store.ID.ToNonAD()
	name := cli.Store.PushName
	if cli.Store.BusinessName != "" {
		name = cli.Store.BusinessName
	}
	pic := m.fetchProfilePic(ctx, cli, jid)

	rt.mu.Lock()
	rt.meta.Owner = cli.Store.ID.User
	if name != "" {
		rt.meta.ProfileName = name
	}
	if pic != "" {
		rt.meta.ProfilePicUrl = pic
	}
	rt.meta.IsBusiness = cli.Store.BusinessName != ""
	in := rt.meta
	rt.mu.Unlock()
	_ = m.store.Save(&in)
}

// ContactProfile captures name + phone + photo for an arbitrary number.
func (m *Manager) ContactProfile(ctx context.Context, id, number string) (map[string]any, error) {
	rt, err := m.requireLoggedIn(id)
	if err != nil {
		return nil, err
	}
	cli := rt.client
	jid, err := m.resolveRecipient(ctx, cli, number) // resolves the canonical JID (handles 9th digit)
	if err != nil {
		return nil, err
	}

	name, isBusiness := "", false
	if c, cerr := cli.Store.Contacts.GetContact(ctx, jid); cerr == nil && c.Found {
		switch {
		case c.FullName != "":
			name = c.FullName
		case c.FirstName != "":
			name = c.FirstName
		case c.PushName != "":
			name = c.PushName
		case c.BusinessName != "":
			name = c.BusinessName
		}
		isBusiness = c.BusinessName != ""
	}
	// Enrich with a live user-info query (verified business name, etc.).
	if infos, uerr := cli.GetUserInfo(ctx, []types.JID{jid}); uerr == nil {
		if ui, ok := infos[jid]; ok && ui.VerifiedName != nil && ui.VerifiedName.Details != nil {
			if vn := ui.VerifiedName.Details.GetVerifiedName(); vn != "" {
				name, isBusiness = vn, true
			}
		}
	}

	return map[string]any{
		"phone":         jid.User,
		"jid":           jid.String(),
		"name":          name,
		"isBusiness":    isBusiness,
		"profilePicUrl": m.fetchProfilePic(ctx, cli, jid),
	}, nil
}

// OwnerProfile re-captures and returns the connected account's profile.
func (m *Manager) OwnerProfile(ctx context.Context, id string) (Instance, error) {
	if _, err := m.requireLoggedIn(id); err != nil {
		return Instance{}, err
	}
	m.captureOwnerProfile(ctx, id)
	return m.Get(id)
}
