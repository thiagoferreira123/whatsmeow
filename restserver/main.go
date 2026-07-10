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
	"strconv"
	"syscall"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver (no CGO), driver name "sqlite"

	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func main() {
	cfg := loadConfig()
	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

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
	mgr.SetRuntimeActive(false)
	hostname, _ := os.Hostname()
	ownerID := hostname + ":" + strconv.Itoa(os.Getpid())
	leaseTTL := time.Duration(cfg.RuntimeLeaseTTLSeconds) * time.Second
	leaseRetry := time.Duration(cfg.RuntimeLeaseRetrySeconds) * time.Second
	lease, err := newRuntimeLease(db, ownerID, leaseTTL)
	if err != nil {
		log.Fatalf("init runtime lease: %v", err)
	}

	handlers := NewHandlers(mgr, cfg)
	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: handlers.Router(),
		// No ReadTimeout: media uploads can be large/slow. Header/idle timeouts
		// still protect against stuck connections exhausting file descriptors.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	log.Printf("whatsmeow REST server listening on :%s (auth=%v autoreply=%v watchdog=%ds connectConcurrency=%d sendConcurrency=%d queueWorkers=%d sendRate=%d/min burst=%d consent=%v)",
		cfg.Port, cfg.AdminAPIKey != "", cfg.AutoReplyEnabled, cfg.WatchdogSeconds, cfg.ConnectConcurrency,
		cfg.GlobalSendConcurrency, cfg.QueueWorkers, cfg.SendRatePerMinute, cfg.SendBurst, cfg.RequireLocalConsent)

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	log.Printf("runtime standby: waiting for exclusive WhatsApp session ownership")
	if err := lease.Wait(ctx, leaseRetry); err != nil {
		log.Printf("runtime lease wait stopped: %v", err)
		shutdownHTTP(srv)
		return
	}
	log.Printf("runtime lease acquired: loading and connecting persisted sessions")
	if err := store.RecoverAfterRuntimeTakeover(); err != nil {
		log.Printf("failed to recover outbound queue: %v", err)
		_ = lease.Release(context.Background())
		shutdownHTTP(srv)
		return
	}
	if err := mgr.LoadAll(ctx); err != nil {
		log.Printf("failed to load existing instances: %v", err)
		_ = lease.Release(context.Background())
		shutdownHTTP(srv)
		return
	}
	mgr.StartQueueWorkers()
	mgr.StartLogCleanup()
	mgr.StartWatchdog(time.Duration(cfg.WatchdogSeconds) * time.Second)
	mgr.SetRuntimeActive(true)

	leaseLost := make(chan error, 1)
	go func() { leaseLost <- lease.Maintain(ctx, leaseRetry) }()
	select {
	case <-ctx.Done():
	case err := <-leaseLost:
		log.Printf("runtime lease lost: %v", err)
	}

	mgr.SetRuntimeActive(false)
	log.Printf("shutting down: draining HTTP and disconnecting clients…")

	shutdownHTTP(srv)
	mgr.Shutdown()
	releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRelease()
	if err := lease.Release(releaseCtx); err != nil {
		log.Printf("release runtime lease: %v", err)
	}
	log.Printf("shutdown complete")
}

func shutdownHTTP(srv *http.Server) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}
