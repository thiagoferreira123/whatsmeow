package main

import (
	"database/sql"
	"errors"
	"time"

	"go.mau.fi/whatsmeow/types"
)

func (s *Store) SaveRecipientAlias(instanceID, resolved, requested string, now time.Time) error {
	resolved = permissionKey(resolved)
	requested = permissionKey(requested)
	if instanceID == "" || resolved == "" || requested == "" {
		return errors.New("recipient alias requires instance, resolved and requested recipients")
	}
	_, err := s.db.Exec(`
		INSERT INTO recipient_aliases (instance_id,resolved_recipient,requested_recipient,updated_at)
		VALUES (?,?,?,?)
		ON CONFLICT(instance_id,resolved_recipient) DO UPDATE SET
			requested_recipient=excluded.requested_recipient,
			updated_at=excluded.updated_at`,
		instanceID, resolved, requested, now.UTC().Format(time.RFC3339Nano),
	)
	return err
}

func (s *Store) GetRequestedRecipientAlias(instanceID, resolved string) (string, error) {
	resolved = permissionKey(resolved)
	if instanceID == "" || resolved == "" {
		return "", nil
	}
	var requested string
	err := s.db.QueryRow(`
		SELECT requested_recipient
		FROM recipient_aliases
		WHERE instance_id=? AND resolved_recipient=?`,
		instanceID, resolved,
	).Scan(&requested)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return requested, err
}

func (m *Manager) rememberRecipientAlias(instanceID, requested string, resolved types.JID) {
	requested = permissionKey(requested)
	resolvedRecipient := permissionKey(resolved.User)
	if requested == "" || resolvedRecipient == "" || requested == resolvedRecipient {
		return
	}
	if err := m.store.SaveRecipientAlias(instanceID, resolvedRecipient, requested, time.Now()); err != nil {
		m.log.Warnf("instance %s: failed to persist recipient alias: %v", instanceID, err)
	}
}

func (m *Manager) canonicalWebhookSenderPN(instanceID string, info types.MessageInfo) string {
	senderPN, _ := resolveSender(info)
	requested, err := m.store.GetRequestedRecipientAlias(instanceID, senderPN)
	if err != nil {
		m.log.Warnf("instance %s: failed to resolve recipient alias: %v", instanceID, err)
		return senderPN
	}
	if requested == "" {
		return senderPN
	}
	return types.NewJID(requested, types.DefaultUserServer).String()
}
