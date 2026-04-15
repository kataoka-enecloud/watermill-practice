// Command watermill-practice is a self-contained demonstration of the
// event-sourced watermill outbox pattern: aggregate → event store →
// outbox → forwarder → NATS → projection → read model → HTTP.
//
// Run:
//
//	docker compose up -d         # postgres :5432 + nats :4222
//	go run .                      # HTTP server on :8080
//
// Drive it:
//
//	curl -X POST localhost:8080/counters/c1/increment -d '{"delta":5}'
//	# → {"counter_id":"c1","version":1}
//
//	curl -X POST localhost:8080/counters/c1/increment -d '{"delta":3}'
//	# → {"counter_id":"c1","version":2}
//
//	curl 'localhost:8080/counters/c1?after=2'
//	# → {"counter_id":"c1","value":8,"version":2}
//
// The ?after=N parameter is a read-your-writes ticket: the GET blocks
// until counter_view.last_event_seq reaches at least N, so a client
// that just saw version 2 from a POST can ask the projection to catch
// up before replying (up to a 5-second budget).
//
// The eight sections of this file, each copyable in isolation:
//
//  1. Event store + read model DDL.
//  2. Minimal event-sourced aggregate.
//  3. newTxEventBus — the three-layer publisher chain
//     (sql.TxFromPgx → forwarder.NewPublisher → cqrs.NewEventBusWithConfig).
//  4. incrementCounter — one pgx transaction: load, execute, append,
//     publish, commit.
//  5. runForwarder — outbox drainer, BeginnerFromPgx + InitializeSchema.
//  6. ensureStream — manual JetStream stream bootstrap (watermill-nats
//     does not create streams for you).
//  7. runProjection — cqrs.EventGroupProcessor + typed handlers.
//  8. HTTP handlers — incrementHTTP + showHTTP + read-your-writes poll.
//
// See watermill-cookbook.md next to this file for prose commentary on
// each recipe and the gotchas each one protects against.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	// watermill core + adapters. wmjs / wsql wrap the underlying
	// brokers in watermill's Publisher/Subscriber interface.
	"github.com/ThreeDotsLabs/watermill"
	wmjs "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/jetstream"
	wsql "github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/components/forwarder"
	"github.com/ThreeDotsLabs/watermill/message"

	// raw drivers. jetsdk is the official NATS SDK's JetStream
	// subpackage, used directly (not via watermill) to bootstrap the
	// stream in ensureStream.
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	natsgo "github.com/nats-io/nats.go"
	jetsdk "github.com/nats-io/nats.go/jetstream"
)

// ======================================================================
// 1. Event store + read model DDL
//
// CREATE IF NOT EXISTS makes this idempotent against a fresh
// docker-compose DB or one that already has the schema.
// ======================================================================

const schemaDDL = `
CREATE TABLE IF NOT EXISTS events (
    stream_id      text        NOT NULL,
    stream_version bigint      NOT NULL,
    event_id       uuid        NOT NULL,
    event_name     text        NOT NULL,
    event_data     jsonb       NOT NULL,
    occurred_at    timestamptz NOT NULL,
    PRIMARY KEY (stream_id, stream_version),
    UNIQUE (event_id)
);

CREATE TABLE IF NOT EXISTS counter_view (
    counter_id     text   PRIMARY KEY,
    value          bigint NOT NULL,
    last_event_seq bigint NOT NULL DEFAULT 0
);
`

// ======================================================================
// 2. Minimal event-sourced aggregate
//
// Counter has no replay helper because the write path does an ad-hoc
// replay in loadCounter below. The goal is to show watermill's moving
// parts, not DDD. Incremented carries its StreamVersion so the event
// store and the projection share one authoritative sequence number.
// ======================================================================

type Incremented struct {
	EventID       uuid.UUID `json:"event_id"`
	CounterID     string    `json:"counter_id"`
	Delta         int64     `json:"delta"`
	StreamVersion int64     `json:"stream_version"`
	OccurredAt    time.Time `json:"occurred_at"`
}

type Counter struct {
	id      string
	value   int64
	version int64
}

func (c *Counter) Apply(e *Incremented) {
	c.value += e.Delta
	c.version = e.StreamVersion
}

func (c *Counter) Increment(delta int64) *Incremented {
	return &Incremented{
		EventID:       uuid.New(),
		CounterID:     c.id,
		Delta:         delta,
		StreamVersion: c.version + 1,
		OccurredAt:    time.Now().UTC(),
	}
}

// ======================================================================
// 3. The three-layer outbox publisher chain — the core recipe
//
// sql.NewPublisher(sql.TxFromPgx(tx)) binds the watermill publisher to
// an already-open pgx transaction, so the outbox INSERT rides the same
// commit/rollback as your domain INSERT. That's the atomicity guarantee
// outbox exists for.
//
// forwarder.NewPublisher wraps the SQL publisher and serialises the
// destination topic ("events" in our case) into a JSON envelope that
// the drainer (section 5) uses to decide where to republish each
// message. Without it, the drainer would not know the NATS subject.
//
// cqrs.NewEventBusWithConfig adds type-based name routing via
// cqrs.JSONMarshaler + cqrs.StructName. Downstream projection handlers
// dispatch by the same rule, so write side and read side agree on
// event names without a shared registry.
//
// See watermill-cookbook.md Recipe 1 (per-tx construction) and
// Recipe 6 (AutoInitializeSchema vs InitializeSchema split) for the
// reasoning behind this exact shape.
// ======================================================================

const (
	outboxTopic = "outbox" // watermill-sql "topic" → physical watermill_outbox table
	eventsTopic = "events" // NATS subject + projection subscribe topic
)

func newTxEventBus(tx pgx.Tx, logger watermill.LoggerAdapter) (*cqrs.EventBus, error) {
	sqlPub, err := wsql.NewPublisher(
		wsql.TxFromPgx(tx),
		wsql.PublisherConfig{SchemaAdapter: wsql.DefaultPostgreSQLSchema{}},
		logger,
	)
	if err != nil {
		return nil, err
	}
	outboxPub := forwarder.NewPublisher(sqlPub, forwarder.PublisherConfig{
		ForwarderTopic: outboxTopic,
	})
	return cqrs.NewEventBusWithConfig(outboxPub, cqrs.EventBusConfig{
		GeneratePublishTopic: func(_ cqrs.GenerateEventPublishTopicParams) (string, error) {
			return eventsTopic, nil
		},
		Marshaler: cqrs.JSONMarshaler{GenerateName: cqrs.StructName},
		Logger:    logger,
	})
}

// ======================================================================
// 4. The write path — four steps inside one transaction
//
//	Begin → { Load → Execute → Append → Publish } → Commit → Apply
//
// The four bracketed steps run inside the tx. If any of them fails,
// the deferred Rollback undoes everything (including the outbox
// INSERT), so there is no "event persisted but not published" state.
// Apply runs after Commit to advance the in-memory aggregate state
// only when the durable write actually landed.
// ======================================================================

func incrementCounter(ctx context.Context, pool *pgxpool.Pool, id string, delta int64, logger watermill.LoggerAdapter) (int64, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	c, err := loadCounter(ctx, tx, id)
	if err != nil {
		return 0, err
	}

	e := c.Increment(delta)

	if err := appendEvent(ctx, tx, id, e); err != nil {
		return 0, err
	}

	// bus only needs the open tx; building it after Append keeps the
	// happy path linear and saves an unused chain on the error path.
	bus, err := newTxEventBus(tx, logger)
	if err != nil {
		return 0, err
	}
	if err := bus.Publish(ctx, e); err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	c.Apply(e)
	return c.version, nil
}

func loadCounter(ctx context.Context, tx pgx.Tx, id string) (*Counter, error) {
	rows, err := tx.Query(ctx, `
		SELECT event_data
		FROM events
		WHERE stream_id = $1
		ORDER BY stream_version ASC`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	c := &Counter{id: id}
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var e Incremented
		if err := json.Unmarshal(data, &e); err != nil {
			return nil, err
		}
		c.Apply(&e)
	}
	return c, rows.Err()
}

func appendEvent(ctx context.Context, tx pgx.Tx, streamID string, e *Incremented) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO events (stream_id, stream_version, event_id, event_name, event_data, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		streamID, e.StreamVersion, e.EventID, "Incremented", data, e.OccurredAt)
	return err
}

// ======================================================================
// 5. The forwarder daemon — drain watermill_outbox into NATS
//
// BeginnerFromPgx, not TxFromPgx: the drainer opens its own short
// transactions per poll, it does not ride any existing one.
//
// InitializeSchema: true creates watermill_outbox and
// watermill_offsets_outbox on first run — the one place schema
// auto-init must be on. Section 1's schemaDDL deliberately does not
// declare the watermill tables; watermill owns their exact shape.
// See cookbook Recipe 2 for the full drainer mechanics.
// ======================================================================

func runForwarder(ctx context.Context, pool *pgxpool.Pool, downstream message.Publisher, logger watermill.LoggerAdapter) error {
	sub, err := wsql.NewSubscriber(
		wsql.BeginnerFromPgx(pool),
		wsql.SubscriberConfig{
			SchemaAdapter:    wsql.DefaultPostgreSQLSchema{},
			OffsetsAdapter:   wsql.DefaultPostgreSQLOffsetsAdapter{},
			InitializeSchema: true,
		},
		logger,
	)
	if err != nil {
		return err
	}
	defer sub.Close()

	fwd, err := forwarder.NewForwarder(sub, downstream, logger, forwarder.Config{
		ForwarderTopic: outboxTopic,
	})
	if err != nil {
		return err
	}
	if err := fwd.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// ======================================================================
// 6. NATS JetStream stream bootstrap
//
// watermill-nats does not create streams for you. If you skip this
// step, both the publisher and the subscriber will silently fail to
// find the stream and your pipeline will appear to run while no
// messages move. Always CreateOrUpdateStream at startup, idempotent.
// ======================================================================

func ensureStream(ctx context.Context, nc *natsgo.Conn, topic string) error {
	js, err := jetsdk.New(nc)
	if err != nil {
		return err
	}
	_, err = js.CreateOrUpdateStream(ctx, jetsdk.StreamConfig{
		Name:     topic,
		Subjects: []string{topic},
	})
	return err
}

// ======================================================================
// 7. The projection loop — NATS → counter_view UPSERT
//
// cqrs.NewGroupEventHandler takes a typed func(ctx, *T) error and
// dispatches by the message's "name" metadata, which the write side
// (section 3) sets via cqrs.StructName. Same marshaler rule on both
// sides means no shared registry.
//
// The projection SQL is a monotonic UPSERT: the WHERE clause makes
// duplicate or out-of-order deliveries silent no-ops, so no separate
// dedup/inbox table is needed.
// ======================================================================

func runProjection(ctx context.Context, nc *natsgo.Conn, pool *pgxpool.Pool, logger watermill.LoggerAdapter) error {
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return err
	}

	eventProc, err := cqrs.NewEventGroupProcessorWithConfig(router, cqrs.EventGroupProcessorConfig{
		GenerateSubscribeTopic: func(_ cqrs.EventGroupProcessorGenerateSubscribeTopicParams) (string, error) {
			return eventsTopic, nil
		},
		SubscriberConstructor: func(_ cqrs.EventGroupProcessorSubscriberConstructorParams) (message.Subscriber, error) {
			return wmjs.NewSubscriber(wmjs.SubscriberConfig{
				Conn:   nc,
				Logger: logger,
			})
		},
		Marshaler: cqrs.JSONMarshaler{GenerateName: cqrs.StructName},
		Logger:    logger,
	})
	if err != nil {
		return err
	}

	if err := eventProc.AddHandlersGroup("counter_view",
		cqrs.NewGroupEventHandler(func(ctx context.Context, e *Incremented) error {
			_, err := pool.Exec(ctx, `
				INSERT INTO counter_view (counter_id, value, last_event_seq)
				VALUES ($1, $2, $3)
				ON CONFLICT (counter_id) DO UPDATE
				SET value          = counter_view.value + EXCLUDED.value,
				    last_event_seq = EXCLUDED.last_event_seq
				WHERE counter_view.last_event_seq < EXCLUDED.last_event_seq`,
				e.CounterID, e.Delta, e.StreamVersion)
			return err
		}),
	); err != nil {
		return err
	}

	return router.Run(ctx)
}

// ======================================================================
// 8. HTTP handlers — the smallest surface for driving the pipeline
//
// POST /counters/{id}/increment  body: {"delta": N}
// GET  /counters/{id}[?after=N]
//
// The GET endpoint accepts ?after=N, a read-your-writes ticket the
// client receives from a previous POST. When set, the handler polls
// counter_view.last_event_seq until it reaches at least N, up to a
// 5-second budget.
// ======================================================================

var errReadTimeout = errors.New("read-your-writes timeout")

func incrementHTTP(pool *pgxpool.Pool, logger watermill.LoggerAdapter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Delta int64 `json:"delta"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if body.Delta <= 0 {
			http.Error(w, "delta must be positive", http.StatusBadRequest)
			return
		}
		id := r.PathValue("id")
		version, err := incrementCounter(r.Context(), pool, id, body.Delta, logger)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprintf(w, `{"counter_id":%q,"version":%d}`, id, version)
	}
}

func showHTTP(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		var after int64
		if s := r.URL.Query().Get("after"); s != "" {
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil || n < 0 {
				http.Error(w, "after must be a non-negative integer", http.StatusBadRequest)
				return
			}
			after = n
		}

		value, version, err := readCounterView(r.Context(), pool, id, after)
		switch {
		case err == nil:
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"counter_id":%q,"value":%d,"version":%d}`, id, value, version)
		case errors.Is(err, errReadTimeout):
			http.Error(w, "read-your-writes timeout", http.StatusRequestTimeout)
		case errors.Is(err, pgx.ErrNoRows):
			http.Error(w, "not found", http.StatusNotFound)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

// readCounterView is the read-your-writes poll used by showHTTP
// (still part of section 8). With after > 0 it polls counter_view
// until last_event_seq reaches at least after or readBudget expires,
// returning errReadTimeout in the latter case. With after == 0 it
// returns immediately, with pgx.ErrNoRows if the counter has no row.
func readCounterView(ctx context.Context, pool *pgxpool.Pool, id string, after int64) (value, version int64, err error) {
	const (
		pollInterval = 25 * time.Millisecond
		readBudget   = 5 * time.Second
	)

	query := func() (int64, int64, error) {
		var v, ver int64
		e := pool.QueryRow(ctx,
			`SELECT value, last_event_seq FROM counter_view WHERE counter_id = $1`, id,
		).Scan(&v, &ver)
		return v, ver, e
	}

	if after == 0 {
		return query()
	}

	deadline := time.Now().Add(readBudget)
	for {
		v, ver, qerr := query()
		// pgx.ErrNoRows here means the projection hasn't created the
		// row yet; treat it as version 0 and keep polling.
		if qerr != nil && !errors.Is(qerr, pgx.ErrNoRows) {
			return 0, 0, qerr
		}
		if qerr == nil && ver >= after {
			return v, ver, nil
		}
		if time.Now().After(deadline) {
			return v, ver, errReadTimeout
		}
		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// ======================================================================
// main — wire everything, start the forwarder, projection, and HTTP
// server, then wait for shutdown
// ======================================================================

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := watermill.NewStdLogger(false, false)

	pool, err := pgxpool.New(ctx, getEnv("POSTGRES_URL", "postgres://app:app@localhost:5432/app"))
	must(err)
	defer pool.Close()

	_, err = pool.Exec(ctx, schemaDDL)
	must(err)

	nc, err := natsgo.Connect(getEnv("NATS_URL", "nats://localhost:4222"))
	must(err)
	defer nc.Close()

	must(ensureStream(ctx, nc, eventsTopic))

	natsPub, err := wmjs.NewPublisher(wmjs.PublisherConfig{
		Conn:   nc,
		Logger: logger,
	})
	must(err)
	defer natsPub.Close()

	go func() {
		if err := runForwarder(ctx, pool, natsPub, logger); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("forwarder: %v", err)
		}
	}()
	go func() {
		if err := runProjection(ctx, nc, pool, logger); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("projection: %v", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /counters/{id}/increment", incrementHTTP(pool, logger))
	mux.HandleFunc("GET /counters/{id}", showHTTP(pool))

	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		log.Println("listening on :8080")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
