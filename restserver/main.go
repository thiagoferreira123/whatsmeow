// Command restserver exposes a REST API over the whatsmeow library so it can
// serve as a local fallback for (and eventual replacement of) the uazapi service.
package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver (no CGO), driver name "sqlite"

	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func main() {
	cfg := loadConfig()
	ctx := context.Background()

	dbLog := waLog.Stdout("DB", "INFO", true)
	waClientLog := waLog.Stdout("WA", "INFO", true)

	// One SQLite file/connection pool shared by whatsmeow's device store and our
	// instances table. Driver name is "sqlite" (modernc); dbutil dialect is "sqlite3".
	db, err := sql.Open("sqlite", cfg.DSN)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()
	// SQLite allows a single writer; a bounded pool keeps memory/fd usage flat
	// under bursts (each conn has its own page cache) while WAL readers stay parallel.
	db.SetMaxOpenConns(cfg.DBMaxConns)
	db.SetMaxIdleConns(cfg.DBMaxConns)

	container := sqlstore.NewWithDB(db, "sqlite3", dbLog)
	if err := container.Upgrade(ctx); err != nil {
		log.Fatalf("upgrade whatsmeow schema: %v", err)
	}

	store, err := NewStore(db)
	if err != nil {
		log.Fatalf("init instances table: %v", err)
	}

	mgr := NewManager(container, store, cfg, waClientLog)
	if err := mgr.LoadAll(ctx); err != nil {
		log.Printf("warning: failed to load existing instances: %v", err)
	}
	mgr.StartWatchdog(time.Duration(cfg.WatchdogSeconds) * time.Second)

	handlers := NewHandlers(mgr, cfg)
	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: handlers.Router(),
		// No ReadTimeout: media uploads can be large/slow. Header/idle timeouts
		// still protect against stuck connections exhausting file descriptors.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	log.Printf("whatsmeow REST server listening on :%s (auth=%v autoreply=%v watchdog=%ds connectConcurrency=%d sendRate=%d/min burst=%d consent=%v)",
		cfg.Port, cfg.AdminAPIKey != "", cfg.AutoReplyEnabled, cfg.WatchdogSeconds, cfg.ConnectConcurrency,
		cfg.SendRatePerMinute, cfg.SendBurst, cfg.RequireLocalConsent)

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	// Graceful shutdown (SIGTERM = Coolify/docker stop): close the websockets
	// cleanly so WhatsApp sees a proper disconnect and re-login on boot is instant.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Printf("shutting down: draining HTTP and disconnecting clients…")

	shutdownCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	mgr.Shutdown()
	log.Printf("shutdown complete")
}
