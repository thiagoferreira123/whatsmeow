package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var (
	logURLPattern    = regexp.MustCompile(`https?://[^\s"'<>]+`)
	logSecretPattern = regexp.MustCompile(`(?i)(token|secret|api[_-]?key|authorization)(\s*[:=]\s*)[^\s,;]+`)
	logBase64Pattern = regexp.MustCompile(`[A-Za-z0-9+/]{96,}={0,2}`)
)

const (
	logCategoryConnection = "connection"
	logCategorySend       = "send"
	logCategoryQueue      = "queue"
	logCategorySystem     = "system"
)

// InstanceLog is a structured, short-lived audit event shown in the panel.
// Message bodies, media payloads, tokens and secrets must never be stored here.
type InstanceLog struct {
	ID          int64          `json:"id"`
	InstanceID  string         `json:"instanceId"`
	Category    string         `json:"category"`
	Event       string         `json:"event"`
	Level       string         `json:"level"`
	Status      string         `json:"status,omitempty"`
	Source      string         `json:"source,omitempty"`
	Recipient   string         `json:"recipient,omitempty"`
	MessageType string         `json:"messageType,omitempty"`
	MessageID   string         `json:"messageId,omitempty"`
	QueueJobID  string         `json:"queueJobId,omitempty"`
	Reason      string         `json:"reason,omitempty"`
	Details     map[string]any `json:"details,omitempty"`
	CreatedAt   string         `json:"createdAt"`
}

type InstanceLogQuery struct {
	BeforeID int64
	Limit    int
	Category string
	Level    string
}

type sendAuditContext struct {
	Source     string
	QueueJobID string
}

type sendAuditContextKey struct{}

func withSendAudit(ctx context.Context, source, queueJobID string) context.Context {
	return context.WithValue(ctx, sendAuditContextKey{}, sendAuditContext{Source: source, QueueJobID: queueJobID})
}

func sendAuditFromContext(ctx context.Context) sendAuditContext {
	if value, ok := ctx.Value(sendAuditContextKey{}).(sendAuditContext); ok {
		if value.Source == "" {
			value.Source = "api"
		}
		return value
	}
	return sendAuditContext{Source: "api"}
}

func (m *Manager) auditSendAttempt(instanceID, recipient, messageType string, audit sendAuditContext) {
	m.auditInstance(instanceID, logCategorySend, "send_attempt", "info", InstanceLog{
		Status: "processing", Source: audit.Source, Recipient: recipient,
		MessageType: messageType, QueueJobID: audit.QueueJobID,
		Reason:  "Solicitação de envio recebida.",
		Details: map[string]any{"attemptedRecipient": recipient},
	})
}

func (m *Manager) auditSendResult(instanceID, attemptedRecipient, resolvedRecipient, messageType, messageID string, sendErr error, audit sendAuditContext) {
	recipient := resolvedRecipient
	if recipient == "" {
		recipient = attemptedRecipient
	}
	entry := InstanceLog{
		Source: audit.Source, Recipient: recipient, MessageType: messageType,
		MessageID: messageID, QueueJobID: audit.QueueJobID,
		Details: map[string]any{"attemptedRecipient": attemptedRecipient},
	}
	if resolvedRecipient != "" {
		entry.Details["resolvedRecipient"] = resolvedRecipient
	}
	if sendErr != nil {
		entry.Status = "failed"
		entry.Reason = sendErr.Error()
		entry.Details["errorType"] = fmt.Sprintf("%T", sendErr)
		m.auditInstance(instanceID, logCategorySend, "send_failed", "error", entry)
		return
	}
	entry.Status = "sent"
	entry.Reason = "Mensagem aceita pelo servidor do WhatsApp."
	m.auditInstance(instanceID, logCategorySend, "send_success", "info", entry)
}

func cleanLogText(value string, max int) string {
	value = strings.TrimSpace(value)
	if max > 0 && len(value) > max {
		return value[:max] + "…"
	}
	return value
}

func sanitizeLogReason(value string) string {
	value = logSecretPattern.ReplaceAllString(value, "$1$2[redacted]")
	value = logBase64Pattern.ReplaceAllString(value, "[large-payload-redacted]")
	value = logURLPattern.ReplaceAllStringFunc(value, func(raw string) string {
		parsed, err := url.Parse(raw)
		if err != nil {
			return "[url-redacted]"
		}
		parsed.RawQuery = ""
		parsed.Fragment = ""
		return parsed.String()
	})
	return cleanLogText(value, 1000)
}

func (s *Store) AddInstanceLog(entry InstanceLog) (int64, error) {
	if entry.InstanceID == "" || entry.Category == "" || entry.Event == "" {
		return 0, fmt.Errorf("instance id, category and event are required")
	}
	if entry.Level == "" {
		entry.Level = "info"
	}
	if entry.CreatedAt == "" {
		entry.CreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	details := "{}"
	if len(entry.Details) > 0 {
		data, err := json.Marshal(entry.Details)
		if err != nil {
			return 0, err
		}
		details = string(data)
	}
	result, err := s.db.Exec(`INSERT INTO instance_logs
		(instance_id,category,event,level,status,source,recipient,message_type,message_id,queue_job_id,reason,details_json,created_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		entry.InstanceID, cleanLogText(entry.Category, 32), cleanLogText(entry.Event, 64), cleanLogText(entry.Level, 16),
		cleanLogText(entry.Status, 32), cleanLogText(entry.Source, 32), cleanLogText(entry.Recipient, 64),
		cleanLogText(entry.MessageType, 32), cleanLogText(entry.MessageID, 128), cleanLogText(entry.QueueJobID, 64),
		sanitizeLogReason(entry.Reason), details, entry.CreatedAt)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func scanInstanceLog(scanner rowScanner) (InstanceLog, error) {
	var entry InstanceLog
	var details string
	err := scanner.Scan(&entry.ID, &entry.InstanceID, &entry.Category, &entry.Event, &entry.Level,
		&entry.Status, &entry.Source, &entry.Recipient, &entry.MessageType, &entry.MessageID,
		&entry.QueueJobID, &entry.Reason, &details, &entry.CreatedAt)
	if err != nil {
		return InstanceLog{}, err
	}
	if details != "" && details != "{}" {
		_ = json.Unmarshal([]byte(details), &entry.Details)
	}
	return entry, nil
}

const instanceLogCols = `id,instance_id,category,event,level,status,source,recipient,message_type,message_id,queue_job_id,reason,details_json,created_at`

func (s *Store) ListInstanceLogs(instanceID string, query InstanceLogQuery) ([]InstanceLog, error) {
	limit := query.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	where := []string{"instance_id=?"}
	args := []any{instanceID}
	if query.BeforeID > 0 {
		where = append(where, "id<?")
		args = append(args, query.BeforeID)
	}
	if query.Category != "" {
		where = append(where, "category=?")
		args = append(args, query.Category)
	}
	if query.Level != "" {
		where = append(where, "level=?")
		args = append(args, query.Level)
	}
	args = append(args, limit)
	rows, err := s.db.Query(`SELECT `+instanceLogCols+` FROM instance_logs WHERE `+
		strings.Join(where, " AND ")+` ORDER BY id DESC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	logs := make([]InstanceLog, 0, limit)
	for rows.Next() {
		entry, err := scanInstanceLog(rows)
		if err != nil {
			return nil, err
		}
		logs = append(logs, entry)
	}
	return logs, rows.Err()
}

func (s *Store) CleanupInstanceLogs(retention time.Duration, now time.Time) (int64, error) {
	if retention <= 0 {
		return 0, nil
	}
	cutoff := now.Add(-retention).UTC().Format(time.RFC3339Nano)
	result, err := s.db.Exec(`DELETE FROM instance_logs WHERE created_at<?`, cutoff)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (m *Manager) auditInstance(instanceID, category, event, level string, entry InstanceLog) {
	if m.store == nil || instanceID == "" || m.get(instanceID) == nil {
		return
	}
	entry.InstanceID = instanceID
	entry.Category = category
	entry.Event = event
	entry.Level = level
	if _, err := m.store.AddInstanceLog(entry); err != nil && m.log != nil {
		m.log.Warnf("instance %s: failed to persist audit event %s: %v", instanceID, event, err)
	}
}

func (m *Manager) StartLogCleanup() {
	retentionDays := m.cfg.InstanceLogRetentionDays
	if retentionDays <= 0 {
		return
	}
	interval := time.Duration(m.cfg.InstanceLogCleanupMinutes) * time.Minute
	if interval <= 0 {
		interval = time.Hour
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.logCleanupCancel = cancel
	m.logCleanupWG.Add(1)
	go func() {
		defer m.logCleanupWG.Done()
		cleanup := func() {
			deleted, err := m.store.CleanupInstanceLogs(time.Duration(retentionDays)*24*time.Hour, time.Now())
			if err != nil {
				m.log.Warnf("instance log cleanup failed: %v", err)
			} else if deleted > 0 {
				m.log.Infof("instance log cleanup removed %d expired rows", deleted)
			}
		}
		cleanup()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cleanup()
			}
		}
	}()
}
