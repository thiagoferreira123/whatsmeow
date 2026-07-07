package main

import (
	"os"
	"strconv"
	"strings"
)

// Config holds all runtime configuration, loaded from environment variables.
type Config struct {
	Port               string
	DSN                string
	AdminAPIKey        string // if empty, auth is disabled (local convenience)
	WebhookSecret      string // default app-secret for the global webhook (HMAC) / legacy x-uazapi-secret
	WebhookURL         string // initial global webhook URL (overridable in the panel)
	WebhookVerifyToken string // global webhook verify token (Cloud API handshake)
	AutoReplyEnabled   bool
	AutoReplyConfirm   string
	AutoReplyCancel    string
	WatchdogSeconds    int // interval of the reconnect watchdog (0 = disabled)
	ConnectConcurrency int // max simultaneous Connect() attempts (boot + watchdog)
	DBMaxConns         int // cap on the SQLite connection pool
}

func loadConfig() Config {
	return Config{
		Port: getenv("PORT", "8080"),
		// WAL + synchronous(NORMAL) for write throughput with many instances;
		// busy_timeout(30000) rides out write bursts (pairing/history-sync storms);
		// _txlock=immediate takes the write lock at BEGIN so concurrent write
		// transactions wait on busy_timeout instead of failing with SQLITE_BUSY.
		DSN:                getenv("WHATSMEOW_DSN", "file:whatsmeow.db?_txlock=immediate&_pragma=foreign_keys(on)&_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"),
		AdminAPIKey:        strings.TrimSpace(os.Getenv("ADMIN_API_KEY")),
		WebhookSecret:      getenv("WEBHOOK_SECRET", "dev-secret"),
		WebhookURL:         strings.TrimSpace(os.Getenv("WEBHOOK_URL")),
		WebhookVerifyToken: strings.TrimSpace(os.Getenv("WEBHOOK_VERIFY_TOKEN")),
		AutoReplyEnabled:   getenvBool("AUTOREPLY_ENABLED", true),
		AutoReplyConfirm:   getenv("AUTOREPLY_CONFIRM_MSG", "✅ Sua consulta foi confirmada! Até breve."),
		AutoReplyCancel:    getenv("AUTOREPLY_CANCEL_MSG", "❌ Sua consulta foi cancelada. Entre em contato para reagendar. \U0001F5D3️"),
		WatchdogSeconds:    getenvInt("WATCHDOG_SECONDS", 30),
		ConnectConcurrency: getenvInt("CONNECT_CONCURRENCY", 8),
		DBMaxConns:         getenvInt("DB_MAX_CONNS", 8),
	}
}

func getenvInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getenv(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func getenvBool(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}
