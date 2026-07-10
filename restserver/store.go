package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"sync/atomic"
	"time"
)

var errNotFound = errors.New("instance not found")

// Instance is the persisted metadata for one WhatsApp account/instance.
// The actual WhatsApp session (keys) lives in whatsmeow's own device store;
// this row maps our public "id" to that device and holds API/webhook config.
type Instance struct {
	ID                         string `json:"id"`
	Name                       string `json:"name"`
	JID                        string `json:"jid,omitempty"`
	Token                      string `json:"token"`
	AdminField01               string `json:"adminField01,omitempty"`
	WebhookURL                 string `json:"webhookUrl,omitempty"`
	WebhookSecret              string `json:"webhookSecret,omitempty"`
	WebhookEvents              string `json:"webhookEvents,omitempty"`
	WebhookExcludeMessages     string `json:"webhookExcludeMessages,omitempty"`
	WebhookEnabled             bool   `json:"webhookEnabled"`
	WebhookAddURLEvents        bool   `json:"webhookAddUrlEvents"`
	WebhookAddURLTypesMessages bool   `json:"webhookAddUrlTypesMessages"`
	Status                     string `json:"status"`
	ProfileName                string `json:"profileName,omitempty"`
	ProfilePicUrl              string `json:"profilePicUrl,omitempty"`
	IsBusiness                 bool   `json:"isBusiness"`
	Owner                      string `json:"owner,omitempty"`
	CreatedAt                  string `json:"createdAt"`
	UpdatedAt                  string `json:"updatedAt"`
	LastDisconnectReason       string `json:"lastDisconnectReason,omitempty"`
	SendingBlockedUntil        string `json:"sendingBlockedUntil,omitempty"`
	LastResetAt                string `json:"lastResetAt,omitempty"`
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS instances (
	id                     TEXT PRIMARY KEY,
	name                   TEXT NOT NULL,
	jid                    TEXT NOT NULL DEFAULT '',
	token                  TEXT NOT NULL,
	admin_field01          TEXT NOT NULL DEFAULT '',
	webhook_url            TEXT NOT NULL DEFAULT '',
	webhook_secret         TEXT NOT NULL DEFAULT '',
	webhook_events         TEXT NOT NULL DEFAULT '',
	webhook_exclude_messages TEXT NOT NULL DEFAULT '',
	webhook_enabled        INTEGER NOT NULL DEFAULT 1,
	webhook_add_url_events INTEGER NOT NULL DEFAULT 0,
	webhook_add_url_types_messages INTEGER NOT NULL DEFAULT 0,
	status                 TEXT NOT NULL DEFAULT 'disconnected',
	profile_name           TEXT NOT NULL DEFAULT '',
	profile_pic_url        TEXT NOT NULL DEFAULT '',
	is_business            INTEGER NOT NULL DEFAULT 0,
	owner                  TEXT NOT NULL DEFAULT '',
	created_at             TEXT NOT NULL,
	updated_at             TEXT NOT NULL,
	last_disconnect_reason TEXT NOT NULL DEFAULT '',
	sending_blocked_until  TEXT NOT NULL DEFAULT '',
	last_reset_at          TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS recipient_permissions (
	instance_id    TEXT NOT NULL,
	recipient      TEXT NOT NULL,
	status         TEXT NOT NULL DEFAULT 'unknown',
	source         TEXT NOT NULL DEFAULT '',
	consented_at   TEXT NOT NULL DEFAULT '',
	revoked_at     TEXT NOT NULL DEFAULT '',
	last_inbound_at TEXT NOT NULL DEFAULT '',
	updated_at     TEXT NOT NULL,
	PRIMARY KEY (instance_id, recipient),
	FOREIGN KEY (instance_id) REFERENCES instances(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS recipient_aliases (
	instance_id        TEXT NOT NULL,
	resolved_recipient TEXT NOT NULL,
	requested_recipient TEXT NOT NULL,
	updated_at         TEXT NOT NULL,
	PRIMARY KEY (instance_id, resolved_recipient),
	FOREIGN KEY (instance_id) REFERENCES instances(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS outbound_activity (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	instance_id TEXT NOT NULL,
	recipient   TEXT NOT NULL,
	sent_at     TEXT NOT NULL,
	FOREIGN KEY (instance_id) REFERENCES instances(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS outbound_activity_recipient_time
	ON outbound_activity(instance_id, recipient, sent_at);
CREATE INDEX IF NOT EXISTS outbound_activity_time ON outbound_activity(sent_at);

CREATE TABLE IF NOT EXISTS instance_logs (
	id           INTEGER PRIMARY KEY AUTOINCREMENT,
	instance_id  TEXT NOT NULL,
	category     TEXT NOT NULL,
	event        TEXT NOT NULL,
	level        TEXT NOT NULL DEFAULT 'info',
	status       TEXT NOT NULL DEFAULT '',
	source       TEXT NOT NULL DEFAULT '',
	recipient    TEXT NOT NULL DEFAULT '',
	message_type TEXT NOT NULL DEFAULT '',
	message_id   TEXT NOT NULL DEFAULT '',
	queue_job_id TEXT NOT NULL DEFAULT '',
	reason       TEXT NOT NULL DEFAULT '',
	details_json TEXT NOT NULL DEFAULT '{}',
	created_at   TEXT NOT NULL,
	FOREIGN KEY (instance_id) REFERENCES instances(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS instance_logs_instance_time
	ON instance_logs(instance_id, id DESC);
CREATE INDEX IF NOT EXISTS instance_logs_created_at
	ON instance_logs(created_at);`

const queueSchemaSQL = `
CREATE TABLE IF NOT EXISTS outbound_queue (
	id              TEXT PRIMARY KEY,
	instance_id     TEXT NOT NULL,
	idempotency_key TEXT NOT NULL,
	kind            TEXT NOT NULL,
	payload_json    TEXT NOT NULL,
	status          TEXT NOT NULL DEFAULT 'queued',
	attempts        INTEGER NOT NULL DEFAULT 0,
	max_attempts    INTEGER NOT NULL DEFAULT 5,
	available_at    TEXT NOT NULL,
	created_at      TEXT NOT NULL,
	updated_at      TEXT NOT NULL,
	message_id      TEXT NOT NULL DEFAULT '',
	last_error      TEXT NOT NULL DEFAULT '',
	FOREIGN KEY (instance_id) REFERENCES instances(id) ON DELETE CASCADE,
	UNIQUE(instance_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS outbound_queue_ready
	ON outbound_queue(status, available_at, created_at);
CREATE INDEX IF NOT EXISTS outbound_queue_instance
	ON outbound_queue(instance_id, status, created_at);`

// Store is a thin CRUD layer over the instances table. It shares the same
// *sql.DB as whatsmeow's sqlstore container (same SQLite file).
type Store struct {
	db             *sql.DB
	outboundWrites atomic.Uint64
}

func NewStore(db *sql.DB) (*Store, error) {
	if _, err := db.Exec(schemaSQL); err != nil {
		return nil, err
	}
	if _, err := db.Exec(queueSchemaSQL); err != nil {
		return nil, err
	}
	// Add columns to pre-existing tables (errors on duplicate column are ignored).
	for _, alter := range []string{
		`ALTER TABLE instances ADD COLUMN profile_pic_url TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE instances ADD COLUMN is_business INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE instances ADD COLUMN sending_blocked_until TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE instances ADD COLUMN last_reset_at TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE instances ADD COLUMN webhook_exclude_messages TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE instances ADD COLUMN webhook_add_url_events INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE instances ADD COLUMN webhook_add_url_types_messages INTEGER NOT NULL DEFAULT 0`,
	} {
		_, _ = db.Exec(alter)
	}
	// Key/value settings (used for the single global webhook config).
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT NOT NULL)`); err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// RecoverAfterRuntimeTakeover mutates queue state only after this process owns
// the runtime lease. A rolling-deploy standby must not reset jobs being handled
// by the still-active process.
func (s *Store) RecoverAfterRuntimeTakeover() error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := s.db.Exec(`UPDATE outbound_queue SET status='queued', available_at=?, updated_at=?, last_error='recovered after process restart' WHERE status='processing'`, now, now); err != nil {
		return err
	}
	cutoff := time.Now().Add(-7 * 24 * time.Hour).UTC().Format(time.RFC3339Nano)
	_, err := s.db.Exec(`DELETE FROM outbound_queue WHERE status IN ('sent','failed','canceled') AND updated_at<?`, cutoff)
	return err
}

// GetSetting returns the value for key, or "" if unset.
func (s *Store) GetSetting(key string) (string, error) {
	var v string
	err := s.db.QueryRow(`SELECT v FROM settings WHERE k=?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

// SetSetting upserts a settings key.
func (s *Store) SetSetting(key, val string) error {
	_, err := s.db.Exec(`INSERT INTO settings (k,v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v`, key, val)
	return err
}

const instanceCols = `id,name,jid,token,admin_field01,webhook_url,webhook_secret,webhook_events,webhook_exclude_messages,webhook_enabled,webhook_add_url_events,webhook_add_url_types_messages,status,profile_name,profile_pic_url,is_business,owner,created_at,updated_at,last_disconnect_reason,sending_blocked_until,last_reset_at`

type rowScanner interface{ Scan(dest ...any) error }

func scanInstance(s rowScanner) (Instance, error) {
	var in Instance
	var enabled, addURLEvents, addURLTypesMessages, business int
	err := s.Scan(
		&in.ID, &in.Name, &in.JID, &in.Token, &in.AdminField01,
		&in.WebhookURL, &in.WebhookSecret, &in.WebhookEvents, &in.WebhookExcludeMessages,
		&enabled, &addURLEvents, &addURLTypesMessages,
		&in.Status, &in.ProfileName, &in.ProfilePicUrl, &business, &in.Owner,
		&in.CreatedAt, &in.UpdatedAt, &in.LastDisconnectReason, &in.SendingBlockedUntil, &in.LastResetAt,
	)
	in.WebhookEnabled = enabled != 0
	in.WebhookAddURLEvents = addURLEvents != 0
	in.WebhookAddURLTypesMessages = addURLTypesMessages != 0
	in.IsBusiness = business != 0
	return in, err
}

func (s *Store) Create(in *Instance) error {
	_, err := s.db.Exec(
		`INSERT INTO instances (`+instanceCols+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		in.ID, in.Name, in.JID, in.Token, in.AdminField01,
		in.WebhookURL, in.WebhookSecret, in.WebhookEvents, in.WebhookExcludeMessages,
		b2i(in.WebhookEnabled), b2i(in.WebhookAddURLEvents), b2i(in.WebhookAddURLTypesMessages),
		in.Status, in.ProfileName, in.ProfilePicUrl, b2i(in.IsBusiness), in.Owner,
		in.CreatedAt, in.UpdatedAt, in.LastDisconnectReason, in.SendingBlockedUntil, in.LastResetAt,
	)
	return err
}

// Save updates every mutable column for the given instance id.
func (s *Store) Save(in *Instance) error {
	in.UpdatedAt = nowRFC()
	_, err := s.db.Exec(
		`UPDATE instances SET name=?,jid=?,token=?,admin_field01=?,webhook_url=?,webhook_secret=?,webhook_events=?,webhook_exclude_messages=?,webhook_enabled=?,webhook_add_url_events=?,webhook_add_url_types_messages=?,status=?,profile_name=?,profile_pic_url=?,is_business=?,owner=?,updated_at=?,last_disconnect_reason=?,sending_blocked_until=?,last_reset_at=? WHERE id=?`,
		in.Name, in.JID, in.Token, in.AdminField01, in.WebhookURL, in.WebhookSecret,
		in.WebhookEvents, in.WebhookExcludeMessages, b2i(in.WebhookEnabled),
		b2i(in.WebhookAddURLEvents), b2i(in.WebhookAddURLTypesMessages),
		in.Status, in.ProfileName, in.ProfilePicUrl,
		b2i(in.IsBusiness), in.Owner, in.UpdatedAt, in.LastDisconnectReason, in.SendingBlockedUntil, in.LastResetAt, in.ID,
	)
	return err
}

func (s *Store) Get(id string) (Instance, error) {
	row := s.db.QueryRow(`SELECT `+instanceCols+` FROM instances WHERE id=?`, id)
	in, err := scanInstance(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Instance{}, errNotFound
	}
	return in, err
}

// GetByToken resolves an instance by its per-instance token (uazapi-compat layer).
func (s *Store) GetByToken(token string) (Instance, error) {
	row := s.db.QueryRow(`SELECT `+instanceCols+` FROM instances WHERE token=?`, token)
	in, err := scanInstance(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Instance{}, errNotFound
	}
	return in, err
}

func (s *Store) List() ([]Instance, error) {
	rows, err := s.db.Query(`SELECT ` + instanceCols + ` FROM instances ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Instance
	for rows.Next() {
		in, err := scanInstance(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, in)
	}
	return out, rows.Err()
}

func (s *Store) Delete(id string) error {
	_, err := s.db.Exec(`DELETE FROM instances WHERE id=?`, id)
	return err
}

// --- small helpers ---

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func nowRFC() string { return time.Now().UTC().Format(time.RFC3339) }

func randToken() string {
	buf := make([]byte, 24)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}
