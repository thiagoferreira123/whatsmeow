package main

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"

	"github.com/google/uuid"
)

// Runtime ownership already excludes other processes; serialize creation within
// this process too. The deterministic ID survives a crash without an extra write.
var instanceCreationMu sync.Mutex

func (m *Manager) CreateIdempotent(key, name, field, url, secret string) (Instance, error) {
	if key == "" {
		return m.Create(name, field, url, secret)
	}
	if len(key) > 200 {
		return Instance{}, &apiError{Status: 400, Msg: "idempotency key too long"}
	}
	instanceCreationMu.Lock()
	defer instanceCreationMu.Unlock()
	hash := sha256.Sum256([]byte("whatsmeow-instance-v1:" + key))
	id, err := uuid.Parse(hex.EncodeToString(hash[:16]))
	if err != nil {
		return Instance{}, err
	}
	existing, err := m.store.Get(id.String())
	if err == nil {
		if existing.Name != name || existing.AdminField01 != field || existing.WebhookURL != url || existing.WebhookSecret != secret {
			return Instance{}, &apiError{Status: 409, Msg: "idempotency key was already used with a different payload"}
		}
		return existing, nil
	}
	if err != errNotFound {
		return Instance{}, err
	}
	return m.createWithID(id.String(), name, field, url, secret)
}
