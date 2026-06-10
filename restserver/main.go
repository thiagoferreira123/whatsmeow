// Command restserver exposes a REST API over the whatsmeow library so it can
// serve as a local fallback for (and eventual replacement of) the uazapi service.
package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
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
	srv := &http.Server{Addr: ":" + cfg.Port, Handler: handlers.Router()}

	log.Printf("whatsmeow REST server listening on :%s (auth=%v autoreply=%v watchdog=%ds)",
		cfg.Port, cfg.AdminAPIKey != "", cfg.AutoReplyEnabled, cfg.WatchdogSeconds)
	log.Fatal(srv.ListenAndServe())
}
