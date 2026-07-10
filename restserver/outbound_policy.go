package main

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strings"
	"sync"
	"time"
)

// RecipientPermission is a local, auditable messaging record. It is not a
// WhatsApp/Meta permission: the unofficial Web protocol exposes no consent API.
// An explicit local opt-out always wins, even when enforcement is disabled.
type RecipientPermission struct {
	InstanceID    string `json:"instanceId"`
	Recipient     string `json:"recipient"`
	Status        string `json:"status"`
	Source        string `json:"source,omitempty"`
	ConsentedAt   string `json:"consentedAt,omitempty"`
	RevokedAt     string `json:"revokedAt,omitempty"`
	LastInboundAt string `json:"lastInboundAt,omitempty"`
	UpdatedAt     string `json:"updatedAt"`
}

func permissionKey(number string) string {
	n := strings.TrimSpace(number)
	if at := strings.IndexByte(n, '@'); at >= 0 {
		n = n[:at]
	}
	return nonDigit.ReplaceAllString(n, "")
}

func (s *Store) SetRecipientConsent(instanceID, recipient, source string, now time.Time) error {
	ts := now.UTC().Format(time.RFC3339)
	_, err := s.db.Exec(`
		INSERT INTO recipient_permissions
			(instance_id,recipient,status,source,consented_at,revoked_at,last_inbound_at,updated_at)
		VALUES (?,?, 'granted', ?, ?, '', '', ?)
		ON CONFLICT(instance_id,recipient) DO UPDATE SET
			status='granted', source=excluded.source, consented_at=excluded.consented_at,
			revoked_at='', updated_at=excluded.updated_at`, instanceID, recipient, source, ts, ts)
	return err
}

func (s *Store) RevokeRecipientConsent(instanceID, recipient, source string, now time.Time) error {
	ts := now.UTC().Format(time.RFC3339)
	_, err := s.db.Exec(`
		INSERT INTO recipient_permissions
			(instance_id,recipient,status,source,consented_at,revoked_at,last_inbound_at,updated_at)
		VALUES (?,?,'revoked',?,'',?,'',?)
		ON CONFLICT(instance_id,recipient) DO UPDATE SET
			status='revoked', source=excluded.source, revoked_at=excluded.revoked_at,
			updated_at=excluded.updated_at`, instanceID, recipient, source, ts, ts)
	return err
}

func (s *Store) RecordInbound(instanceID, recipient string, now time.Time) error {
	ts := now.UTC().Format(time.RFC3339)
	_, err := s.db.Exec(`
		INSERT INTO recipient_permissions
			(instance_id,recipient,status,source,consented_at,revoked_at,last_inbound_at,updated_at)
		VALUES (?,?,'unknown','','','',?,?)
		ON CONFLICT(instance_id,recipient) DO UPDATE SET
			last_inbound_at=excluded.last_inbound_at, updated_at=excluded.updated_at`, instanceID, recipient, ts, ts)
	return err
}

func (s *Store) GetRecipientPermission(instanceID, recipient string) (RecipientPermission, error) {
	var p RecipientPermission
	err := s.db.QueryRow(`SELECT instance_id,recipient,status,source,consented_at,revoked_at,last_inbound_at,updated_at
		FROM recipient_permissions WHERE instance_id=? AND recipient=?`, instanceID, recipient).Scan(
		&p.InstanceID, &p.Recipient, &p.Status, &p.Source, &p.ConsentedAt,
		&p.RevokedAt, &p.LastInboundAt, &p.UpdatedAt,
	)
	return p, err
}

func (s *Store) RecordOutbound(instanceID, recipient string, now time.Time) error {
	_, err := s.db.Exec(`INSERT INTO outbound_activity(instance_id,recipient,sent_at) VALUES (?,?,?)`,
		instanceID, recipient, now.UTC().Format(time.RFC3339Nano))
	if err == nil && s.outboundWrites.Add(1)%1000 == 0 {
		_, _ = s.db.Exec(`DELETE FROM outbound_activity WHERE sent_at<?`,
			now.Add(-7*24*time.Hour).UTC().Format(time.RFC3339Nano))
	}
	return err
}

func (s *Store) OutboundWindow(instanceID, recipient string, since time.Time) (count int, oldest time.Time, err error) {
	var oldestText sql.NullString
	err = s.db.QueryRow(`SELECT COUNT(*), MIN(sent_at) FROM outbound_activity
		WHERE instance_id=? AND recipient=? AND sent_at>=?`, instanceID, recipient,
		since.UTC().Format(time.RFC3339Nano)).Scan(&count, &oldestText)
	if err != nil || !oldestText.Valid {
		return count, time.Time{}, err
	}
	oldest, err = time.Parse(time.RFC3339Nano, oldestText.String)
	return count, oldest, err
}

type tokenBucket struct {
	tokens float64
	last   time.Time
}

// outboundGuard is an in-memory admission controller. The rolling per-recipient
// daily cap and consent state are persisted separately in SQLite.
type outboundGuard struct {
	mu         sync.Mutex
	buckets    map[string]*tokenBucket
	recipients map[string]time.Time
	rate       int
	burst      int
	cooldown   time.Duration
}

func newOutboundGuard(cfg Config) *outboundGuard {
	burst := cfg.SendBurst
	if burst <= 0 {
		burst = 1
	}
	return &outboundGuard{
		buckets: make(map[string]*tokenBucket), recipients: make(map[string]time.Time),
		rate: cfg.SendRatePerMinute, burst: burst,
		cooldown: time.Duration(cfg.RecipientCooldown) * time.Second,
	}
}

func (g *outboundGuard) allow(instanceID, recipient string, now time.Time) time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	key := instanceID + "|" + recipient
	if last, ok := g.recipients[key]; ok && g.cooldown > 0 {
		if wait := g.cooldown - now.Sub(last); wait > 0 {
			return wait
		}
	}
	if g.rate > 0 {
		b := g.buckets[instanceID]
		if b == nil {
			b = &tokenBucket{tokens: float64(g.burst), last: now}
			g.buckets[instanceID] = b
		}
		elapsed := now.Sub(b.last).Seconds()
		b.tokens = math.Min(float64(g.burst), b.tokens+elapsed*float64(g.rate)/60)
		b.last = now
		if b.tokens < 1 {
			return time.Duration(math.Ceil((1-b.tokens)*60/float64(g.rate)*1000)) * time.Millisecond
		}
		b.tokens--
	}
	g.recipients[key] = now
	return 0
}

func parseStoredTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	t, _ := time.Parse(time.RFC3339, value)
	return t
}

func (m *Manager) checkOutbound(ctx context.Context, instanceID, recipient string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := time.Now()
	if rt := m.get(instanceID); rt != nil {
		rt.mu.RLock()
		blockedUntil := parseStoredTime(rt.meta.SendingBlockedUntil)
		rt.mu.RUnlock()
		if !blockedUntil.IsZero() && now.Before(blockedUntil) {
			return rateLimitError("outbound circuit breaker is active after a temporary ban", time.Until(blockedUntil))
		}
	}
	p, err := m.store.GetRecipientPermission(instanceID, recipient)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil && p.Status == "revoked" {
		return &apiError{Status: 403, Msg: "recipient opted out; outbound messaging is blocked"}
	}
	if m.cfg.RequireLocalConsent {
		withinServiceWindow := err == nil && !parseStoredTime(p.LastInboundAt).IsZero() &&
			now.Sub(parseStoredTime(p.LastInboundAt)) <= time.Duration(m.cfg.ServiceWindowHours)*time.Hour
		if err != nil || (p.Status != "granted" && !withinServiceWindow) {
			return &apiError{Status: 403, Msg: "recipient has no local consent record or recent inbound service window"}
		}
	}
	if m.cfg.RecipientDailyMax > 0 {
		count, oldest, err := m.store.OutboundWindow(instanceID, recipient, now.Add(-24*time.Hour))
		if err != nil {
			return err
		}
		if count >= m.cfg.RecipientDailyMax {
			wait := time.Until(oldest.Add(24 * time.Hour))
			return rateLimitError("recipient rolling 24h message limit reached", wait)
		}
	}
	if wait := m.outbound.allow(instanceID, recipient, now); wait > 0 {
		return rateLimitError("outbound cadence limit reached", wait)
	}
	return nil
}

func rateLimitError(msg string, wait time.Duration) error {
	seconds := int(math.Ceil(wait.Seconds()))
	if seconds < 1 {
		seconds = 1
	}
	return &apiError{Status: 429, Msg: msg, RetryAfter: seconds}
}

func (m *Manager) GrantConsent(instanceID, number, source string) (RecipientPermission, error) {
	if m.get(instanceID) == nil {
		return RecipientPermission{}, errNotFound
	}
	recipient := permissionKey(number)
	if recipient == "" {
		return RecipientPermission{}, &apiError{Status: 400, Msg: "invalid recipient"}
	}
	if strings.TrimSpace(source) == "" {
		return RecipientPermission{}, &apiError{Status: 400, Msg: "consent source is required"}
	}
	now := time.Now()
	for _, key := range phoneCandidates(recipient) {
		if err := m.store.SetRecipientConsent(instanceID, key, strings.TrimSpace(source), now); err != nil {
			return RecipientPermission{}, err
		}
	}
	return m.store.GetRecipientPermission(instanceID, recipient)
}

func (m *Manager) RevokeConsent(instanceID, number, source string) (RecipientPermission, error) {
	if m.get(instanceID) == nil {
		return RecipientPermission{}, errNotFound
	}
	recipient := permissionKey(number)
	if recipient == "" {
		return RecipientPermission{}, &apiError{Status: 400, Msg: "invalid recipient"}
	}
	if strings.TrimSpace(source) == "" {
		source = "api"
	}
	now := time.Now()
	for _, key := range phoneCandidates(recipient) {
		if err := m.store.RevokeRecipientConsent(instanceID, key, strings.TrimSpace(source), now); err != nil {
			return RecipientPermission{}, err
		}
	}
	return m.store.GetRecipientPermission(instanceID, recipient)
}

func (m *Manager) GetConsent(instanceID, number string) (RecipientPermission, error) {
	if m.get(instanceID) == nil {
		return RecipientPermission{}, errNotFound
	}
	recipient := permissionKey(number)
	if recipient == "" {
		return RecipientPermission{}, &apiError{Status: 400, Msg: "invalid recipient"}
	}
	for _, key := range phoneCandidates(recipient) {
		p, err := m.store.GetRecipientPermission(instanceID, key)
		if err == nil {
			return p, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return RecipientPermission{}, err
		}
	}
	return RecipientPermission{InstanceID: instanceID, Recipient: recipient, Status: "unknown"}, nil
}
