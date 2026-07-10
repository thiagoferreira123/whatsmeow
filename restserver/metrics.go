package main

import (
	"fmt"
	"strings"
	"sync/atomic"
)

type managerStats struct {
	connectAttempts atomic.Uint64
	connectFailures atomic.Uint64
	sendSuccess     atomic.Uint64
	sendFailures    atomic.Uint64
	queueEnqueued   atomic.Uint64
	queueSent       atomic.Uint64
	queueErrors     atomic.Uint64
	resets          atomic.Uint64
}

func (m *Manager) metricsText() string {
	m.mu.RLock()
	runtimes := make([]*instanceRuntime, 0, len(m.runtimes))
	for _, rt := range m.runtimes {
		runtimes = append(runtimes, rt)
	}
	m.mu.RUnlock()
	var connected, connecting, hibernated, disconnected int
	for _, rt := range runtimes {
		switch m.statusOf(rt) {
		case "connected":
			connected++
		case "connecting":
			connecting++
		case "hibernated":
			hibernated++
		default:
			disconnected++
		}
	}
	lines := []string{
		"# TYPE whatsmeow_instances gauge",
		fmt.Sprintf("whatsmeow_instances{status=\"connected\"} %d", connected),
		fmt.Sprintf("whatsmeow_instances{status=\"connecting\"} %d", connecting),
		fmt.Sprintf("whatsmeow_instances{status=\"hibernated\"} %d", hibernated),
		fmt.Sprintf("whatsmeow_instances{status=\"disconnected\"} %d", disconnected),
		"# TYPE whatsmeow_connect_attempts_total counter",
		fmt.Sprintf("whatsmeow_connect_attempts_total %d", m.stats.connectAttempts.Load()),
		"# TYPE whatsmeow_connect_failures_total counter",
		fmt.Sprintf("whatsmeow_connect_failures_total %d", m.stats.connectFailures.Load()),
		"# TYPE whatsmeow_send_success_total counter",
		fmt.Sprintf("whatsmeow_send_success_total %d", m.stats.sendSuccess.Load()),
		"# TYPE whatsmeow_send_failures_total counter",
		fmt.Sprintf("whatsmeow_send_failures_total %d", m.stats.sendFailures.Load()),
		"# TYPE whatsmeow_queue_enqueued_total counter",
		fmt.Sprintf("whatsmeow_queue_enqueued_total %d", m.stats.queueEnqueued.Load()),
		"# TYPE whatsmeow_queue_sent_total counter",
		fmt.Sprintf("whatsmeow_queue_sent_total %d", m.stats.queueSent.Load()),
		"# TYPE whatsmeow_queue_errors_total counter",
		fmt.Sprintf("whatsmeow_queue_errors_total %d", m.stats.queueErrors.Load()),
		"# TYPE whatsmeow_runtime_resets_total counter",
		fmt.Sprintf("whatsmeow_runtime_resets_total %d", m.stats.resets.Load()),
	}
	if counts, err := m.store.GlobalQueueCounts(); err == nil {
		lines = append(lines, "# TYPE whatsmeow_queue_jobs gauge")
		for _, status := range []string{queueQueued, queueWaitingConnection, queueProcessing, queueSent, queueFailed, queueCanceled} {
			lines = append(lines, fmt.Sprintf("whatsmeow_queue_jobs{status=\"%s\"} %d", status, counts[status]))
		}
	}
	return strings.Join(lines, "\n") + "\n"
}
