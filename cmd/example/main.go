// Command example runs a local HTTP service with the novaque admin UI and a
// demo consumer. Use it to exercise publish/subscribe against MySQL.
//
//	go run ./cmd/example
//
// With an existing database:
//
//	NOVAQUE_MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/novaque?parseTime=true&loc=UTC' \
//	  go run ./cmd/example -addr :8080
//
// Try it:
//
//	curl -sS -X POST http://127.0.0.1:8080/demo/publish -d 'hello from curl'
//	curl -sS http://127.0.0.1:8080/demo/stats
//	open http://127.0.0.1:8080/admin/
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/usual2970/novaque"
	"github.com/usual2970/novaque/admin"
	mysqldriver "github.com/usual2970/novaque/driver/mysql"
)

func main() {
	var (
		addr     = flag.String("addr", "127.0.0.1:9080", "HTTP listen address")
		dsnFlag  = flag.String("dsn", "", "MySQL DSN (or NOVAQUE_MYSQL_DSN); empty starts testcontainers")
		topic    = flag.String("topic", "demo", "demo publish/subscribe topic")
		channel  = flag.String("channel", "workers", "demo channel name")
		pool     = flag.Int("pool", 32, "sql.DB MaxOpenConns")
		authUser = flag.String("admin-user", "", "optional admin basic-auth user (both user and pass required)")
		authPass = flag.String("admin-pass", "", "optional admin basic-auth password")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn, cleanup := resolveDSN(ctx, *dsnFlag)
	defer cleanup()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(*pool)
	db.SetMaxIdleConns(*pool)
	db.SetConnMaxLifetime(time.Minute)
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("ping mysql: %v", err)
	}

	client, err := novaque.Open(mysqldriver.New(db), novaque.Options{
		DefaultTTL:   24 * time.Hour,
		DefaultLease: 30 * time.Second,
		PollInterval: 100 * time.Millisecond,
		MaxInFlight:  4,
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := client.Migrate(ctx); err != nil {
		log.Fatal(err)
	}
	if err := client.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer client.Shutdown(context.Background())

	var consumed atomic.Int64
	var lastBody atomic.Value // string

	cons, err := client.SubscribeAndStart(ctx, *topic, *channel, func(_ context.Context, msg *novaque.Message) error {
		consumed.Add(1)
		lastBody.Store(string(msg.Body))
		log.Printf("consumed topic=%q channel=%q body=%q attempts=%d", *topic, *channel, msg.Body, msg.Attempts)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cons.Shutdown(context.Background())

	adminHandler, err := admin.New(client, admin.Options{
		Prefix:        "/admin",
		BasicAuthUser: *authUser,
		BasicAuthPass: *authPass,
	})
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle("/admin/", adminHandler)
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/admin/", http.StatusFound)
	})
	mux.HandleFunc("POST /demo/publish", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(body) == 0 {
			http.Error(w, "empty body", http.StatusBadRequest)
			return
		}
		id, err := client.Publish(r.Context(), *topic, body, novaque.PublishOpts{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"message_id": id,
			"topic":      *topic,
			"channels":   []string{*channel},
		})
	})
	mux.HandleFunc("GET /demo/stats", func(w http.ResponseWriter, r *http.Request) {
		bl, err := client.ChannelBacklog(r.Context(), *topic, *channel)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		last, _ := lastBody.Load().(string)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"topic":             *topic,
			"channel":           *channel,
			"consumed_total":    consumed.Load(),
			"last_body":         last,
			"backlog_pending":   bl.Pending,
			"backlog_ready":     bl.Ready,
			"backlog_in_flight": bl.InFlight,
			"backlog_dead":      bl.Dead,
		})
	})

	srv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("novaque example listening on http://%s", *addr)
	log.Printf("  admin:   http://%s/admin/", *addr)
	log.Printf("  publish: curl -X POST http://%s/demo/publish -d 'hello'", *addr)
	log.Printf("  stats:   curl http://%s/demo/stats", *addr)
	log.Printf("  topic=%q channel=%q", *topic, *channel)

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	fmt.Println("shutdown complete")
}

func resolveDSN(ctx context.Context, flagDSN string) (string, func()) {
	if flagDSN == "" {
		flagDSN = os.Getenv("NOVAQUE_MYSQL_DSN")
	}
	if flagDSN != "" {
		return flagDSN, func() {}
	}
	log.Print("starting MySQL 8 via testcontainers…")
	req := testcontainers.ContainerRequest{
		Image:        "mysql:8.0.36",
		Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "root", "MYSQL_DATABASE": "novaque"},
		ExposedPorts: []string{"3306/tcp"},
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").
			WithStartupTimeout(2 * time.Minute),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatalf("testcontainers: %v", err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		log.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "3306")
	if err != nil {
		log.Fatal(err)
	}
	dsn := fmt.Sprintf("root:root@tcp(%s:%s)/novaque?parseTime=true&loc=UTC&multiStatements=true", host, port.Port())
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		db, err := sql.Open("mysql", dsn)
		if err == nil {
			pingErr := db.Ping()
			_ = db.Close()
			if pingErr == nil {
				log.Printf("mysql ready at %s:%s", host, port.Port())
				return dsn, func() { _ = c.Terminate(context.Background()) }
			}
		}
		time.Sleep(400 * time.Millisecond)
	}
	_ = c.Terminate(context.Background())
	log.Fatal("mysql container not ready")
	return "", nil
}
