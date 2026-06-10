package main

import (
	"os"
	"strconv"
	"strings"
)

// Config holds all runtime configuration, loaded from environment variables.
type Config struct {
	Port             string
	DSN              string
	AdminAPIKey      string // if empty, auth is disabled (local convenience)
	WebhookSecret    string // default app-secret for the global webhook (HMAC) / legacy x-uazapi-secret
	WebhookURL         string // initial global webhook URL (overridable in the panel)
	WebhookVerifyToken string // global webhook verify token (Cloud API handshake)
	AutoReplyEnabled bool
	AutoReplyConfirm string
	AutoReplyCancel  string
	WatchdogSeconds  int // interval of the reconnect watchdog (0 = disabled)
}

func loadConfig() Config {
	return Config{
		Port: getenv("PORT", "8080"),
		// WAL improves durability/concurrency vs the default rollback journal.
		DSN:              getenv("WHATSMEOW_DSN", "file:whatsmeow.db?_pragma=foreign_keys(on)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"),
		AdminAPIKey:        strings.TrimSpace(os.Getenv("ADMIN_API_KEY")),
		WebhookSecret:      getenv("WEBHOOK_SECRET", "dev-secret"),
		WebhookURL:         strings.TrimSpace(os.Getenv("WEBHOOK_URL")),
		WebhookVerifyToken: strings.TrimSpace(os.Getenv("WEBHOOK_VERIFY_TOKEN")),
		AutoReplyEnabled:   getenvBool("AUTOREPLY_ENABLED", true),
		AutoReplyConfirm: getenv("AUTOREPLY_CONFIRM_MSG", "✅ Sua consulta foi confirmada! Até breve."),
		AutoReplyCancel:  getenv("AUTOREPLY_CANCEL_MSG", "❌ Sua consulta foi cancelada. Entre em contato para reagendar. \U0001F5D3️"),
		WatchdogSeconds:  getenvInt("WATCHDOG_SECONDS", 30),
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
