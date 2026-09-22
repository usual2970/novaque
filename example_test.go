// The examples in this file demonstrate the primary embedder flows. Each one
// needs a live MySQL at the DSN below, so none carry an Output comment: go
// test compiles them without running. With a running database, every example
// completes as its body comments describe.
package novaque_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/usual2970/novaque"

	"github.com/usual2970/novaque/driver/mysql"
)

func ExampleOpen() {
	db, err := sql.Open("mysql", "user:pass@tcp(127.0.0.1:3306)/app?parseTime=true&loc=UTC")
	if err != nil {
		log.Fatal(err)
	}

	// Wrap the host-owned *sql.DB; fields left zero keep their defaults
	// (DefaultTTL, DefaultLease, DefaultMaxAttempts, ...).
	client, err := novaque.Open(mysql.New(db), novaque.Options{
		MaxInFlight: 8, // concurrent handler workers per consumer
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := client.Migrate(ctx); err != nil { // schema setup; idempotent, safe every startup
		log.Fatal(err)
	}
	if err := client.Start(ctx); err != nil { // lease reaper + TTL purge + stats loops
		log.Fatal(err)
	}
	defer client.Shutdown(context.Background()) // one final best-effort FlushStats
	// With a live database this process runs the maintenance loops until
	// shutdown; publishing and consuming build on the started client below.
}

func ExampleClient_Publish() {
	// Schema assumed migrated once at startup (see ExampleOpen).
	db, err := sql.Open("mysql", "user:pass@tcp(127.0.0.1:3306)/app?parseTime=true&loc=UTC")
	if err != nil {
		log.Fatal(err)
	}
	client, err := novaque.Open(mysql.New(db), novaque.Options{})
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	messageID, err := client.Publish(ctx, "events", []byte(`{"kind":"user.created"}`), novaque.PublishOpts{
		TTL:         24 * time.Hour,   // retention from publish time; zero would mean DefaultTTL (7d)
		Delay:       30 * time.Minute, // claimable in 30 minutes, not before
		MaxAttempts: 10,               // poison threshold; zero would mean DefaultMaxAttempts (5)
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("published message %d", messageID)
	// With a live database the message fans out to one delivery per channel
	// subscribed to "events" — the topic is created on demand if new — each
	// claimable 30 minutes from now and expiring after 24 hours. Channels
	// subscribed later do not receive this message retroactively.
}

func ExampleClient_SubscribeAndStart() {
	db, err := sql.Open("mysql", "user:pass@tcp(127.0.0.1:3306)/app?parseTime=true&loc=UTC")
	if err != nil {
		log.Fatal(err)
	}
	client, err := novaque.Open(mysql.New(db), novaque.Options{})
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := client.Start(ctx); err != nil { // maintenance loops; consuming works without them too
		log.Fatal(err)
	}

	// One consumer on ("events", "indexer"); MaxInFlight workers compete
	// within the channel.
	handler := func(ctx context.Context, msg *novaque.Message) error {
		// Handlers must be idempotent: delivery is at-least-once, so the
		// same message may arrive more than once. ctx is bounded by the
		// lease timeout, not by the ctx passed to SubscribeAndStart.
		log.Printf("indexing message %d (attempt %d)", msg.MessageID, msg.Attempts)
		return nil // nil acks; a non-nil error requeues for redelivery
	}
	consumer, err := client.SubscribeAndStart(ctx, "events", "indexer", handler)
	if err != nil {
		log.Fatal(err)
	}

	if _, err := client.Publish(ctx, "events", []byte(`{"kind":"user.created"}`), novaque.PublishOpts{}); err != nil {
		log.Fatal(err)
	}
	// With a live database the handler receives the message within a poll
	// interval (~200ms base with ±50% jitter) and acks it.

	// Stop the consumer before the client so its final acks land first.
	if err := consumer.Shutdown(context.Background()); err != nil {
		log.Fatal(err)
	}
	if err := client.Shutdown(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_ChannelBacklog() {
	db, err := sql.Open("mysql", "user:pass@tcp(127.0.0.1:3306)/app?parseTime=true&loc=UTC")
	if err != nil {
		log.Fatal(err)
	}
	client, err := novaque.Open(mysql.New(db), novaque.Options{})
	if err != nil {
		log.Fatal(err)
	}

	// Live counts for one channel right now. The call also creates the
	// channel if it does not exist yet — it then joins future fan-out.
	backlog, err := client.ChannelBacklog(context.Background(), "events", "indexer")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("pending=%d ready=%d in_flight=%d dead=%d\n",
		backlog.Pending, backlog.Ready, backlog.InFlight, backlog.Dead)
	// With a live database holding one published, not-yet-claimed message
	// this prints: pending=1 ready=1 in_flight=0 dead=0
}
