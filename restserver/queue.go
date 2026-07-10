package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	queueQueued            = "queued"
	queueWaitingConnection = "waiting_connection"
	queueProcessing        = "processing"
	queueSent              = "sent"
	queueFailed            = "failed"
	queueCanceled          = "canceled"
)

type QueueJob struct {
	ID             string `json:"id"`
	InstanceID     string `json:"instanceId"`
	IdempotencyKey string `json:"idempotencyKey"`
	Kind           string `json:"kind"`
	PayloadJSON    string `json:"-"`
	Status         string `json:"status"`
	Attempts       int    `json:"attempts"`
	MaxAttempts    int    `json:"maxAttempts"`
	AvailableAt    string `json:"availableAt"`
	CreatedAt      string `json:"createdAt"`
	UpdatedAt      string `json:"updatedAt"`
	MessageID      string `json:"messageId,omitempty"`
	LastError      string `json:"lastError,omitempty"`
}

type queuedTextPayload struct {
	Number string `json:"number"`
	Text   string `json:"text"`
}

type queuedMediaPayload struct {
	Number   string `json:"number"`
	Type     string `json:"type,omitempty"`
	File     string `json:"file"`
	Text     string `json:"text,omitempty"`
	FileName string `json:"fileName,omitempty"`
}

func scanQueueJob(s rowScanner) (QueueJob, error) {
	var j QueueJob
	err := s.Scan(&j.ID, &j.InstanceID, &j.IdempotencyKey, &j.Kind, &j.PayloadJSON,
		&j.Status, &j.Attempts, &j.MaxAttempts, &j.AvailableAt, &j.CreatedAt,
		&j.UpdatedAt, &j.MessageID, &j.LastError)
	return j, err
}

const queueCols = `id,instance_id,idempotency_key,kind,payload_json,status,attempts,max_attempts,available_at,created_at,updated_at,message_id,last_error`

func (s *Store) Enqueue(instanceID, key, kind string, payload any, maxAttempts int) (QueueJob, bool, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return QueueJob{}, false, err
	}
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	id := uuid.NewString()
	key = strings.TrimSpace(key)
	if key == "" {
		key = id
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.db.Exec(`INSERT OR IGNORE INTO outbound_queue
		(id,instance_id,idempotency_key,kind,payload_json,status,attempts,max_attempts,available_at,created_at,updated_at)
		VALUES (?,?,?,?,?,'queued',0,?,?,?,?)`, id, instanceID, key, kind, string(data), maxAttempts, now, now, now)
	if err != nil {
		return QueueJob{}, false, err
	}
	rows, _ := res.RowsAffected()
	job, err := s.GetQueueByKey(instanceID, key)
	if err == nil && rows == 0 && (job.Kind != kind || job.PayloadJSON != string(data)) {
		return QueueJob{}, false, &apiError{Status: 409, Msg: "idempotency key was already used with a different payload"}
	}
	return job, rows > 0, err
}

func (s *Store) GetQueueByKey(instanceID, key string) (QueueJob, error) {
	return scanQueueJob(s.db.QueryRow(`SELECT `+queueCols+` FROM outbound_queue WHERE instance_id=? AND idempotency_key=?`, instanceID, key))
}

func (s *Store) ClaimQueueJob(ctx context.Context, now time.Time) (QueueJob, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return QueueJob{}, err
	}
	defer tx.Rollback()
	nowText := now.UTC().Format(time.RFC3339Nano)
	job, err := scanQueueJob(tx.QueryRowContext(ctx, `SELECT `+queueCols+` FROM outbound_queue q
		WHERE q.status IN ('queued','waiting_connection') AND q.available_at<=?
		AND NOT EXISTS (SELECT 1 FROM outbound_queue active
			WHERE active.instance_id=q.instance_id AND active.status='processing')
		ORDER BY q.available_at,q.created_at LIMIT 1`, nowText))
	if err != nil {
		return QueueJob{}, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE outbound_queue SET status='processing',updated_at=?
		WHERE id=? AND status IN ('queued','waiting_connection')`, nowText, job.ID)
	if err != nil {
		return QueueJob{}, err
	}
	rows, _ := res.RowsAffected()
	if rows != 1 {
		return QueueJob{}, sql.ErrNoRows
	}
	if err := tx.Commit(); err != nil {
		return QueueJob{}, err
	}
	job.Status = queueProcessing
	job.UpdatedAt = nowText
	return job, nil
}

func (s *Store) FinishQueue(id, messageID string, now time.Time) error {
	_, err := s.db.Exec(`UPDATE outbound_queue SET status='sent',message_id=?,last_error='',updated_at=? WHERE id=?`,
		messageID, now.UTC().Format(time.RFC3339Nano), id)
	return err
}

func (s *Store) FailQueue(id, lastError string, now time.Time) error {
	_, err := s.db.Exec(`UPDATE outbound_queue SET status='failed',attempts=attempts+1,last_error=?,updated_at=? WHERE id=?`,
		lastError, now.UTC().Format(time.RFC3339Nano), id)
	return err
}

func (s *Store) DeferQueue(job QueueJob, status, lastError string, availableAt time.Time, incrementAttempt bool) (bool, error) {
	attempts := job.Attempts
	if incrementAttempt {
		attempts++
	}
	if attempts >= job.MaxAttempts {
		return true, s.FailQueue(job.ID, lastError, time.Now())
	}
	if status != queueQueued && status != queueWaitingConnection {
		status = queueQueued
	}
	_, err := s.db.Exec(`UPDATE outbound_queue SET status=?,attempts=?,available_at=?,last_error=?,updated_at=? WHERE id=?`,
		status, attempts, availableAt.UTC().Format(time.RFC3339Nano), lastError,
		time.Now().UTC().Format(time.RFC3339Nano), job.ID)
	return false, err
}

func (s *Store) CancelQueue(instanceID string) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.db.Exec(`UPDATE outbound_queue SET status='canceled',updated_at=?,last_error='canceled by operator'
		WHERE instance_id=? AND status IN ('queued','waiting_connection')`, now, instanceID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) RecoverInstanceQueue(instanceID string) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.db.Exec(`UPDATE outbound_queue SET status='queued',available_at=?,updated_at=?,last_error='recovered by runtime reset'
		WHERE instance_id=? AND status='waiting_connection'`, now, now, instanceID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) QueueSummary(instanceID string) (map[string]any, error) {
	rows, err := s.db.Query(`SELECT status,COUNT(*) FROM outbound_queue WHERE instance_id=? GROUP BY status`, instanceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]int64{}
	var pending int64
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		counts[status] = count
		if status == queueQueued || status == queueWaitingConnection || status == queueProcessing {
			pending += count
		}
	}
	return map[string]any{"pending": pending, "counts": counts}, rows.Err()
}

func (s *Store) GlobalQueueCounts() (map[string]int64, error) {
	rows, err := s.db.Query(`SELECT status,COUNT(*) FROM outbound_queue GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]int64{}
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		counts[status] = count
	}
	return counts, rows.Err()
}

func (s *Store) ListQueue(instanceID, status string, limit int) ([]QueueJob, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := `SELECT ` + queueCols + ` FROM outbound_queue WHERE instance_id=?`
	args := []any{instanceID}
	if status != "" {
		query += ` AND status=?`
		args = append(args, status)
	}
	query += ` ORDER BY created_at DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QueueJob
	for rows.Next() {
		job, err := scanQueueJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, job)
	}
	return out, rows.Err()
}

func (m *Manager) EnqueueText(instanceID, number, text, key string) (QueueJob, bool, error) {
	return m.EnqueueTextFrom(instanceID, number, text, key, "api")
}

func (m *Manager) EnqueueTextFrom(instanceID, number, text, key, source string) (QueueJob, bool, error) {
	if m.get(instanceID) == nil {
		return QueueJob{}, false, errNotFound
	}
	if strings.TrimSpace(number) == "" || strings.TrimSpace(text) == "" {
		return QueueJob{}, false, &apiError{Status: 400, Msg: "number and text are required"}
	}
	job, created, err := m.store.Enqueue(instanceID, key, "text", queuedTextPayload{Number: number, Text: text}, m.cfg.QueueMaxAttempts)
	if created {
		m.stats.queueEnqueued.Add(1)
		m.auditInstance(instanceID, logCategoryQueue, "message_enqueued", "info", InstanceLog{
			Status: queueQueued, Source: source, Recipient: permissionKey(number), MessageType: "text", QueueJobID: job.ID,
		})
	}
	return job, created, err
}

func (m *Manager) EnqueueMedia(instanceID string, payload queuedMediaPayload, key string) (QueueJob, bool, error) {
	return m.EnqueueMediaFrom(instanceID, payload, key, "api")
}

func (m *Manager) EnqueueMediaFrom(instanceID string, payload queuedMediaPayload, key, source string) (QueueJob, bool, error) {
	if m.get(instanceID) == nil {
		return QueueJob{}, false, errNotFound
	}
	if strings.TrimSpace(payload.Number) == "" || strings.TrimSpace(payload.File) == "" {
		return QueueJob{}, false, &apiError{Status: 400, Msg: "number and file are required"}
	}
	job, created, err := m.store.Enqueue(instanceID, key, "media", payload, m.cfg.QueueMaxAttempts)
	if created {
		m.stats.queueEnqueued.Add(1)
		messageType := payload.Type
		if messageType == "" {
			messageType = "media"
		}
		m.auditInstance(instanceID, logCategoryQueue, "message_enqueued", "info", InstanceLog{
			Status: queueQueued, Source: source, Recipient: permissionKey(payload.Number), MessageType: messageType, QueueJobID: job.ID,
		})
	}
	return job, created, err
}

func (m *Manager) StartQueueWorkers() {
	workers := m.cfg.QueueWorkers
	if workers <= 0 {
		return
	}
	poll := time.Duration(m.cfg.QueuePollMilliseconds) * time.Millisecond
	if poll <= 0 {
		poll = 500 * time.Millisecond
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.queueCancel = cancel
	for i := 0; i < workers; i++ {
		m.queueWG.Add(1)
		go func() {
			defer m.queueWG.Done()
			m.queueWorker(ctx, poll)
		}()
	}
}

func (m *Manager) queueWorker(ctx context.Context, poll time.Duration) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		job, err := m.store.ClaimQueueJob(ctx, time.Now())
		if errors.Is(err, sql.ErrNoRows) {
			timer.Reset(poll)
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.log.Warnf("queue claim failed: %v", err)
			timer.Reset(time.Second)
			continue
		}
		m.processQueueJob(ctx, job)
		timer.Reset(0)
	}
}

func (m *Manager) processQueueJob(ctx context.Context, job QueueJob) {
	var auditPayload struct {
		Number string `json:"number"`
	}
	_ = json.Unmarshal([]byte(job.PayloadJSON), &auditPayload)
	recipient := permissionKey(auditPayload.Number)
	rt := m.get(job.InstanceID)
	if rt == nil {
		_ = m.store.FailQueue(job.ID, "instance no longer exists", time.Now())
		return
	}
	rt.mu.RLock()
	cli := rt.client
	paused := rt.paused || rt.conflicted
	rt.mu.RUnlock()
	if cli == nil || !cli.IsLoggedIn() || !cli.IsConnected() || paused {
		_, _ = m.store.DeferQueue(job, queueWaitingConnection, "session is not ready", time.Now().Add(10*time.Second), false)
		if job.LastError != "session is not ready" {
			m.auditInstance(job.InstanceID, logCategoryQueue, "waiting_connection", "warning", InstanceLog{
				Status: queueWaitingConnection, Source: "queue", Recipient: recipient, MessageType: job.Kind, QueueJobID: job.ID,
				Reason: "session is not ready",
			})
		}
		return
	}
	var messageID string
	var err error
	switch job.Kind {
	case "text":
		var payload queuedTextPayload
		if err = json.Unmarshal([]byte(job.PayloadJSON), &payload); err == nil {
			messageID, err = m.SendText(withSendAudit(ctx, "queue", job.ID), job.InstanceID, payload.Number, payload.Text)
		}
	case "media":
		var payload queuedMediaPayload
		if err = json.Unmarshal([]byte(job.PayloadJSON), &payload); err == nil {
			messageID, err = m.SendMedia(withSendAudit(ctx, "queue", job.ID), job.InstanceID, payload.Number, payload.Type, payload.File, payload.Text, payload.FileName)
		}
	default:
		err = fmt.Errorf("unsupported queue kind %q", job.Kind)
	}
	if err == nil {
		_ = m.store.FinishQueue(job.ID, messageID, time.Now())
		m.stats.queueSent.Add(1)
		m.auditInstance(job.InstanceID, logCategoryQueue, "queue_completed", "info", InstanceLog{
			Status: queueSent, Source: "queue", Recipient: recipient, MessageType: job.Kind, MessageID: messageID, QueueJobID: job.ID,
		})
		return
	}
	m.stats.queueErrors.Add(1)
	if errors.Is(err, errNotConnected) {
		_, _ = m.store.DeferQueue(job, queueWaitingConnection, err.Error(), time.Now().Add(10*time.Second), false)
		return
	}
	var ae *apiError
	if errors.As(err, &ae) {
		switch {
		case ae.Status == 429:
			wait := time.Duration(ae.RetryAfter) * time.Second
			if wait <= 0 {
				wait = 10 * time.Second
			}
			_, _ = m.store.DeferQueue(job, queueQueued, err.Error(), time.Now().Add(wait), false)
			m.auditInstance(job.InstanceID, logCategoryQueue, "retry_scheduled", "warning", InstanceLog{
				Status: queueQueued, Source: "queue", Recipient: recipient, MessageType: job.Kind, QueueJobID: job.ID, Reason: err.Error(),
				Details: map[string]any{"nextAttemptInSeconds": int(wait.Seconds()), "rateLimited": true},
			})
			return
		case ae.Status >= 400 && ae.Status < 500:
			_ = m.store.FailQueue(job.ID, err.Error(), time.Now())
			m.auditInstance(job.InstanceID, logCategoryQueue, "queue_failed", "error", InstanceLog{
				Status: queueFailed, Source: "queue", Recipient: recipient, MessageType: job.Kind, QueueJobID: job.ID, Reason: err.Error(),
			})
			return
		}
	}
	seconds := math.Min(float64(m.cfg.QueueRetryMaxSeconds), math.Pow(2, float64(job.Attempts))*5)
	if seconds < 5 {
		seconds = 5
	}
	terminal, _ := m.store.DeferQueue(job, queueQueued, err.Error(), time.Now().Add(time.Duration(seconds)*time.Second), true)
	if terminal {
		m.log.Warnf("queue job %s failed permanently after %d attempts: %v", job.ID, job.MaxAttempts, err)
		m.auditInstance(job.InstanceID, logCategoryQueue, "queue_failed", "error", InstanceLog{
			Status: queueFailed, Source: "queue", Recipient: recipient, MessageType: job.Kind, QueueJobID: job.ID, Reason: err.Error(),
			Details: map[string]any{"attempts": job.MaxAttempts},
		})
	} else {
		m.auditInstance(job.InstanceID, logCategoryQueue, "retry_scheduled", "warning", InstanceLog{
			Status: queueQueued, Source: "queue", Recipient: recipient, MessageType: job.Kind, QueueJobID: job.ID, Reason: err.Error(),
			Details: map[string]any{"nextAttemptInSeconds": int(seconds), "attempt": job.Attempts + 1},
		})
	}
}
