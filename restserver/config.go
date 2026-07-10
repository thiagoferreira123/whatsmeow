package main

import (
	"os"
	"strconv"
	"strings"
)

// Config holds all runtime configuration, loaded from environment variables.
type Config struct {
	Port                      string
	DSN                       string
	AdminAPIKey               string // if empty, auth is disabled (local convenience)
	WebhookSecret             string // default app-secret for the global webhook (HMAC) / legacy x-uazapi-secret
	WebhookURL                string // initial global webhook URL (overridable in the panel)
	WebhookVerifyToken        string // global webhook verify token (Cloud API handshake)
	UazapiCompatWebhookURL    string // default per-instance DietSystem webhook for uazapi-compatible instances
	AutoReplyEnabled          bool
	AutoReplyConfirm          string
	AutoReplyCancel           string
	WatchdogSeconds           int  // interval of the reconnect watchdog (0 = disabled)
	ConnectConcurrency        int  // max simultaneous Connect() attempts (boot + watchdog)
	DBMaxConns                int  // cap on the SQLite connection pool
	SendRatePerMinute         int  // sustained outbound rate per instance (0 = disabled)
	SendBurst                 int  // token-bucket burst per instance
	RecipientCooldown         int  // minimum seconds between outbound messages to one recipient
	RecipientDailyMax         int  // max outbound messages to one recipient in a rolling 24h window
	RequireLocalConsent       bool // require the local consent ledger or a recent inbound service window
	ServiceWindowHours        int  // an inbound message authorizes replies for this many hours
	GlobalSendConcurrency     int  // max simultaneous outbound operations across all instances
	QueueWorkers              int  // persistent outbound queue workers
	QueuePollMilliseconds     int  // idle queue polling interval
	QueueMaxAttempts          int  // terminal failure threshold for queued messages
	QueueRetryMaxSeconds      int  // maximum retry backoff for transient send failures
	ResetCooldownSeconds      int  // minimum interval between controlled runtime resets
	InstanceLogRetentionDays  int  // structured per-instance audit retention
	InstanceLogCleanupMinutes int  // cleanup worker interval
}

func loadConfig() Config {
	return Config{
		Port: getenv("PORT", "8080"),
		// WAL + synchronous(NORMAL) for write throughput with many instances;
		// busy_timeout(30000) rides out write bursts (pairing/history-sync storms);
		// _txlock=immediate takes the write lock at BEGIN so concurrent write
		// transactions wait on busy_timeout instead of failing with SQLITE_BUSY.
		DSN:                    getenv("WHATSMEOW_DSN", "file:whatsmeow.db?_txlock=immediate&_pragma=foreign_keys(on)&_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"),
		AdminAPIKey:            strings.TrimSpace(os.Getenv("ADMIN_API_KEY")),
		WebhookSecret:          getenv("WEBHOOK_SECRET", "dev-secret"),
		WebhookURL:             strings.TrimSpace(os.Getenv("WEBHOOK_URL")),
		WebhookVerifyToken:     strings.TrimSpace(os.Getenv("WEBHOOK_VERIFY_TOKEN")),
		UazapiCompatWebhookURL: strings.TrimSpace(os.Getenv("UAZAPI_COMPAT_WEBHOOK_URL")),
		AutoReplyEnabled:       getenvBool("AUTOREPLY_ENABLED", true),
		AutoReplyConfirm:       getenv("AUTOREPLY_CONFIRM_MSG", "✅ Sua consulta foi confirmada! Até breve."),
		AutoReplyCancel:        getenv("AUTOREPLY_CANCEL_MSG", "❌ Sua consulta foi cancelada. Entre em contato para reagendar. \U0001F5D3️"),
		WatchdogSeconds:        getenvInt("WATCHDOG_SECONDS", 30),
		ConnectConcurrency:     getenvInt("CONNECT_CONCURRENCY", 8),
		DBMaxConns:             getenvInt("DB_MAX_CONNS", 8),
		SendRatePerMinute:      getenvInt("SEND_RATE_PER_MINUTE", 30),
		SendBurst:              getenvInt("SEND_BURST", 5),
		RecipientCooldown:      getenvInt("SEND_RECIPIENT_COOLDOWN_SECONDS", 10),
		RecipientDailyMax:      getenvInt("SEND_RECIPIENT_DAILY_MAX", 20),
		// SEND_REQUIRE_CONSENT is kept as a backwards-compatible alias. This is
		// strictly a local ledger; the unofficial Web client cannot query a
		// WhatsApp/Meta consent state.
		RequireLocalConsent:       getenvBool("SEND_REQUIRE_LOCAL_CONSENT", getenvBool("SEND_REQUIRE_CONSENT", false)),
		ServiceWindowHours:        getenvInt("SEND_SERVICE_WINDOW_HOURS", 24),
		GlobalSendConcurrency:     getenvInt("GLOBAL_SEND_CONCURRENCY", 8),
		QueueWorkers:              getenvInt("QUEUE_WORKERS", 4),
		QueuePollMilliseconds:     getenvInt("QUEUE_POLL_MILLISECONDS", 500),
		QueueMaxAttempts:          getenvInt("QUEUE_MAX_ATTEMPTS", 5),
		QueueRetryMaxSeconds:      getenvInt("QUEUE_RETRY_MAX_SECONDS", 300),
		ResetCooldownSeconds:      getenvInt("RESET_COOLDOWN_SECONDS", 60),
		InstanceLogRetentionDays:  getenvInt("INSTANCE_LOG_RETENTION_DAYS", 7),
		InstanceLogCleanupMinutes: getenvInt("INSTANCE_LOG_CLEANUP_INTERVAL_MINUTES", 60),
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
