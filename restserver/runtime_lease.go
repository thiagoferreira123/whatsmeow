package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const runtimeLeaseName = "whatsapp-sessions"

type runtimeLease struct {
	db      *sql.DB
	ownerID string
	ttl     time.Duration
}

func newRuntimeLease(db *sql.DB, ownerID string, ttl time.Duration) (*runtimeLease, error) {
	if db == nil {
		return nil, errors.New("runtime lease requires a database")
	}
	if ownerID == "" {
		return nil, errors.New("runtime lease requires an owner id")
	}
	if ttl <= 0 {
		return nil, errors.New("runtime lease TTL must be positive")
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS runtime_leases (
		name TEXT PRIMARY KEY,
		owner_id TEXT NOT NULL,
		expires_at INTEGER NOT NULL,
		takeover_owner TEXT NOT NULL DEFAULT ''
	)`); err != nil {
		return nil, fmt.Errorf("create runtime lease table: %w", err)
	}
	return &runtimeLease{db: db, ownerID: ownerID, ttl: ttl}, nil
}

func (l *runtimeLease) TryAcquire(ctx context.Context) (bool, error) {
	now := time.Now()
	result, err := l.db.ExecContext(ctx, `
		INSERT INTO runtime_leases(name, owner_id, expires_at)
		VALUES (?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			owner_id=excluded.owner_id,
			expires_at=excluded.expires_at,
			takeover_owner=''
		WHERE runtime_leases.owner_id=excluded.owner_id
		   OR runtime_leases.expires_at<=?`,
		runtimeLeaseName, l.ownerID, now.Add(l.ttl).UnixMilli(), now.UnixMilli(),
	)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil || affected == 1 {
		return affected == 1, err
	}
	_, err = l.db.ExecContext(ctx, `
		UPDATE runtime_leases SET takeover_owner=?
		WHERE name=? AND owner_id<>? AND (takeover_owner='' OR takeover_owner=?)`,
		l.ownerID, runtimeLeaseName, l.ownerID, l.ownerID,
	)
	return false, err
}

func (l *runtimeLease) Renew(ctx context.Context) (bool, error) {
	result, err := l.db.ExecContext(ctx,
		`UPDATE runtime_leases SET expires_at=? WHERE name=? AND owner_id=? AND takeover_owner=''`,
		time.Now().Add(l.ttl).UnixMilli(), runtimeLeaseName, l.ownerID,
	)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected == 1, err
}

func (l *runtimeLease) Release(ctx context.Context) error {
	_, err := l.db.ExecContext(ctx,
		`DELETE FROM runtime_leases WHERE name=? AND owner_id=?`,
		runtimeLeaseName, l.ownerID,
	)
	return err
}

func (l *runtimeLease) Wait(ctx context.Context, retry time.Duration) error {
	if retry <= 0 {
		retry = time.Second
	}
	for {
		acquired, err := l.TryAcquire(ctx)
		if err == nil && acquired {
			return nil
		}
		timer := time.NewTimer(retry)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (l *runtimeLease) Maintain(ctx context.Context, interval time.Duration) error {
	if interval <= 0 || interval >= l.ttl {
		interval = l.ttl / 3
	}
	lastRenewed := time.Now()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			renewed, err := l.Renew(ctx)
			if err == nil && renewed {
				lastRenewed = time.Now()
				continue
			}
			if err == nil && !renewed {
				return errors.New("runtime lease ownership lost")
			}
			if time.Since(lastRenewed) >= l.ttl {
				return fmt.Errorf("runtime lease renewal failed for %s: %w", l.ttl, err)
			}
		}
	}
}
