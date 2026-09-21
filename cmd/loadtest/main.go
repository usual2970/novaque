// Command loadtest runs a local publish/consume stress test against MySQL.
//
//	go run ./cmd/loadtest
//	NOVAQUE_MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/novaque?parseTime=true&loc=UTC' go run ./cmd/loadtest -n 10000
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"novaque"
	mysqldriver "novaque/driver/mysql"
)

func main() {
	var (
		n           = flag.Int("n", 5000, "total messages to publish")
		publishers  = flag.Int("publishers", 8, "concurrent publishers")
		maxInFlight = flag.Int("max-inflight", 32, "consumer MaxInFlight")
		bodySize    = flag.Int("body", 64, "message body bytes")
		dsnFlag     = flag.String("dsn", "", "MySQL DSN (or NOVAQUE_MYSQL_DSN); empty starts testcontainers")
		pool        = flag.Int("pool", 64, "sql.DB MaxOpenConns")
	)
	flag.Parse()

	ctx := context.Background()
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
		DefaultTTL:       time.Hour,
		DefaultLease:     30 * time.Second,
		PollInterval:     20 * time.Millisecond,
		MaxInFlight:      *maxInFlight,
		ReapInterval:     time.Second,
		PurgeInterval:    time.Hour,
		MaintenanceBatch: 200,
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

	topic := fmt.Sprintf("load_%d", time.Now().UnixNano())
	body := make([]byte, *bodySize)
	for i := range body {
		body[i] = byte('a' + i%26)
	}

	var consumed atomic.Int64
	var once sync.Once
	done := make(chan struct{})
	target := int64(*n)

	cons, err := client.SubscribeAndStart(ctx, topic, "workers", func(_ context.Context, _ *novaque.Message) error {
		if consumed.Add(1) >= target {
			once.Do(func() { close(done) })
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cons.Shutdown(context.Background())

	// Let poller start before flooding publishes.
	time.Sleep(100 * time.Millisecond)

	log.Printf("loadtest n=%d publishers=%d maxInFlight=%d body=%dB pool=%d topic=%s",
		*n, *publishers, *maxInFlight, *bodySize, *pool, topic)

	pubStart := time.Now()
	var pubErr atomic.Value
	var wg sync.WaitGroup
	jobs := make(chan int, *n)
	for i := 0; i < *n; i++ {
		jobs <- i
	}
	close(jobs)
	for p := 0; p < *publishers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				if _, err := client.Publish(ctx, topic, body, novaque.PublishOpts{}); err != nil {
					pubErr.Store(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	pubDur := time.Since(pubStart)
	if v := pubErr.Load(); v != nil {
		log.Fatalf("publish: %v", v)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Minute):
		log.Fatalf("timeout: consumed=%d/%d", consumed.Load(), target)
	}
	totalDur := time.Since(pubStart)

	fmt.Println()
	fmt.Printf("publish:  %d msgs in %s  (%.0f msg/s)\n", *n, pubDur.Round(time.Millisecond), float64(*n)/pubDur.Seconds())
	fmt.Printf("pipeline: %d msgs in %s  (%.0f msg/s end-to-end)\n", *n, totalDur.Round(time.Millisecond), float64(*n)/totalDur.Seconds())
	fmt.Printf("consumed: %d\n", consumed.Load())
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
