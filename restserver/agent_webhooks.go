package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const agentWebhookSchema = `CREATE TABLE IF NOT EXISTS agent_webhook_outbox (
 id TEXT PRIMARY KEY, instance_id TEXT NOT NULL, body BLOB, status TEXT NOT NULL DEFAULT 'pending',
 attempts INTEGER NOT NULL DEFAULT 0, available_at INTEGER NOT NULL DEFAULT 0,
 created_at INTEGER NOT NULL, delivered_at INTEGER
);`

func (m *Manager) agentCipher() (cipher.AEAD, error) {
	if m.cfg.AdminAPIKey == "" {
		return nil, fmt.Errorf("agent webhook encryption unavailable")
	}
	key := sha256.Sum256([]byte("agent-webhooks-v1:" + m.cfg.AdminAPIKey))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// Only the new support instance opts in. Other webhook consumers keep their contract.
func (m *Manager) enqueueAgentWebhook(url string, payload any) bool {
	data, ok := payload.(map[string]any)
	if !ok || data["instanceName"] != "agendamento_bot" || !strings.HasSuffix(url, "/v1/whatsmeow") {
		return false
	}
	instance, ok := data["instance"].(map[string]any)
	if !ok {
		return false
	}
	instanceID, _ := instance["id"].(string)
	message, ok := data["message"].(map[string]any)
	if !ok {
		return false
	}
	messageID, _ := message["messageid"].(string)
	if messageID == "" {
		return false
	}
	// Never store tokens or the redundant operator display name in the delivery queue.
	cleanMessage := make(map[string]any)
	for k, v := range message {
		if k != "pushName" {
			cleanMessage[k] = v
		}
	}
	clean := map[string]any{"EventType": "messages", "instanceName": "agendamento_bot", "message": cleanMessage}
	body, err := json.Marshal(clean)
	aead, cipherErr := m.agentCipher()
	if err != nil || cipherErr != nil {
		m.log.Errorf("agent webhook cannot be persisted")
		return true
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return true
	}
	encrypted := aead.Seal(nonce, nonce, body, []byte(instanceID))
	digest := sha256.Sum256([]byte(instanceID + ":" + messageID))
	_, err = m.store.db.Exec(agentWebhookSchema)
	if err == nil {
		_, err = m.store.db.Exec(`INSERT INTO agent_webhook_outbox(id,instance_id,body,created_at) VALUES(?,?,?,?) ON CONFLICT(id) DO NOTHING`, hex.EncodeToString(digest[:]), instanceID, encrypted, time.Now().Unix())
	}
	if err != nil {
		m.log.Errorf("agent webhook persist failed")
	}
	return true
}

func (m *Manager) dispatchAgentWebhook() bool {
	var id, instanceID string
	var encrypted []byte
	var attempts int
	err := m.store.db.QueryRow(`SELECT id,instance_id,body,attempts FROM agent_webhook_outbox WHERE status='pending' AND available_at<=? ORDER BY created_at,id LIMIT 1`, time.Now().Unix()).Scan(&id, &instanceID, &encrypted, &attempts)
	if err != nil {
		return false
	}
	rt := m.get(instanceID)
	if rt == nil {
		return false
	}
	in := rt.metaCopy()
	if in.Name != "agendamento_bot" || !in.WebhookEnabled || !strings.HasSuffix(in.WebhookURL, "/v1/whatsmeow") {
		return false
	}
	aead, err := m.agentCipher()
	if err != nil || len(encrypted) < aead.NonceSize() {
		return false
	}
	body, err := aead.Open(nil, encrypted[:aead.NonceSize()], encrypted[aead.NonceSize():], []byte(instanceID))
	if err != nil {
		return false
	}
	out := m.webhooks.deliverSync(in.WebhookURL, webhookSecretFor(in, m.cfg), body)
	if out.Delivered {
		_, err = m.store.db.Exec(`UPDATE agent_webhook_outbox SET status='delivered',body=NULL,delivered_at=? WHERE id=?`, time.Now().Unix(), id)
	} else {
		delay := min(300, 5*(attempts+1))
		_, err = m.store.db.Exec(`UPDATE agent_webhook_outbox SET attempts=attempts+1,available_at=? WHERE id=?`, time.Now().Unix()+int64(delay), id)
		m.log.Warnf("agent webhook retry scheduled (status=%d)", out.StatusCode)
	}
	if err != nil {
		m.log.Errorf("agent webhook acknowledgement persist failed")
	}
	return true
}

func (m *Manager) StartAgentWebhooks(ctx context.Context) {
	if _, err := m.store.db.Exec(agentWebhookSchema); err != nil {
		m.log.Errorf("agent webhook queue unavailable")
		return
	}
	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				for n := 0; n < 10; n++ {
					if ctx.Err() != nil || !m.dispatchAgentWebhook() {
						break
					}
				}
				_, _ = m.store.db.Exec(`DELETE FROM agent_webhook_outbox WHERE status='delivered' AND delivered_at<?`, time.Now().Add(-30*24*time.Hour).Unix())
			}
		}
	}()
}
