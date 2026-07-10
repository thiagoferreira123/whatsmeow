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
	ID                   string `json:"id"`
	Name                 string `json:"name"`
	JID                  string `json:"jid,omitempty"`
	Token                string `json:"token"`
	AdminField01         string `json:"adminField01,omitempty"`
	WebhookURL           string `json:"webhookUrl,omitempty"`
	WebhookSecret        string `json:"webhookSecret,omitempty"`
	WebhookEvents        string `json:"webhookEvents,omitempty"`
	WebhookEnabled       bool   `json:"webhookEnabled"`
	Status               string `json:"status"`
	ProfileName          string `json:"profileName,omitempty"`
	ProfilePicUrl        string `json:"profilePicUrl,omitempty"`
	IsBusiness           bool   `json:"isBusiness"`
	Owner                string `json:"owner,omitempty"`
	CreatedAt            string `json:"createdAt"`
	UpdatedAt            string `json:"updatedAt"`
	LastDisconnectReason string `json:"lastDisconnectReason,omitempty"`
	SendingBlockedUntil  string `json:"sendingBlockedUntil,omitempty"`
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
	webhook_enabled        INTEGER NOT NULL DEFAULT 1,
	status                 TEXT NOT NULL DEFAULT 'disconnected',
	profile_name           TEXT NOT NULL DEFAULT '',
	profile_pic_url        TEXT NOT NULL DEFAULT '',
	is_business            INTEGER NOT NULL DEFAULT 0,
	owner                  TEXT NOT NULL DEFAULT '',
	created_at             TEXT NOT NULL,
	updated_at             TEXT NOT NULL,
	last_disconnect_reason TEXT NOT NULL DEFAULT '',
	sending_blocked_until  TEXT NOT NULL DEFAULT ''
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

CREATE TABLE IF NOT EXISTS outbound_activity (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	instance_id TEXT NOT NULL,
	recipient   TEXT NOT NULL,
	sent_at     TEXT NOT NULL,
	FOREIGN KEY (instance_id) REFERENCES instances(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS outbound_activity_recipient_time
	ON outbound_activity(instance_id, recipient, sent_at);
CREATE INDEX IF NOT EXISTS outbound_activity_time ON outbound_activity(sent_at);`

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
	// Add columns to pre-existing tables (errors on duplicate column are ignored).
	for _, alter := range []string{
		`ALTER TABLE instances ADD COLUMN profile_pic_url TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE instances ADD COLUMN is_business INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE instances ADD COLUMN sending_blocked_until TEXT NOT NULL DEFAULT ''`,
	} {
		_, _ = db.Exec(alter)
	}
	// Key/value settings (used for the single global webhook config).
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT NOT NULL)`); err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
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

const instanceCols = `id,name,jid,token,admin_field01,webhook_url,webhook_secret,webhook_events,webhook_enabled,status,profile_name,profile_pic_url,is_business,owner,created_at,updated_at,last_disconnect_reason,sending_blocked_until`

type rowScanner interface{ Scan(dest ...any) error }

func scanInstance(s rowScanner) (Instance, error) {
	var in Instance
	var enabled, business int
	err := s.Scan(
		&in.ID, &in.Name, &in.JID, &in.Token, &in.AdminField01,
		&in.WebhookURL, &in.WebhookSecret, &in.WebhookEvents, &enabled,
		&in.Status, &in.ProfileName, &in.ProfilePicUrl, &business, &in.Owner,
		&in.CreatedAt, &in.UpdatedAt, &in.LastDisconnectReason, &in.SendingBlockedUntil,
	)
	in.WebhookEnabled = enabled != 0
	in.IsBusiness = business != 0
	return in, err
}

func (s *Store) Create(in *Instance) error {
	_, err := s.db.Exec(
		`INSERT INTO instances (`+instanceCols+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		in.ID, in.Name, in.JID, in.Token, in.AdminField01,
		in.WebhookURL, in.WebhookSecret, in.WebhookEvents, b2i(in.WebhookEnabled),
		in.Status, in.ProfileName, in.ProfilePicUrl, b2i(in.IsBusiness), in.Owner,
		in.CreatedAt, in.UpdatedAt, in.LastDisconnectReason, in.SendingBlockedUntil,
	)
	return err
}

// Save updates every mutable column for the given instance id.
func (s *Store) Save(in *Instance) error {
	in.UpdatedAt = nowRFC()
	_, err := s.db.Exec(
		`UPDATE instances SET name=?,jid=?,token=?,admin_field01=?,webhook_url=?,webhook_secret=?,webhook_events=?,webhook_enabled=?,status=?,profile_name=?,profile_pic_url=?,is_business=?,owner=?,updated_at=?,last_disconnect_reason=?,sending_blocked_until=? WHERE id=?`,
		in.Name, in.JID, in.Token, in.AdminField01, in.WebhookURL, in.WebhookSecret,
		in.WebhookEvents, b2i(in.WebhookEnabled), in.Status, in.ProfileName, in.ProfilePicUrl,
		b2i(in.IsBusiness), in.Owner, in.UpdatedAt, in.LastDisconnectReason, in.SendingBlockedUntil, in.ID,
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
