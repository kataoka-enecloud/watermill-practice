// Command watermill-practice is a self-contained demonstration of the
// [Transactional Outbox pattern] applied with Watermill: aggregate →
// event store → outbox → forwarder → NATS → projection → read model → HTTP.
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
// The ?after=N parameter is a read-your-writes ticket (see [Werner Vogels
// on read-your-writes]): the GET blocks until counter_view.last_event_seq
// reaches at least N, so a client that just saw version 2 from a POST can
// ask the projection to catch up before replying (up to a 5-second budget).
//
// The eight numbered sections of this file:
//
//  1. Event store + read model DDL.
//  2. Counter aggregate.
//  3. app struct + (*app).newTxEventBus — the three-layer publisher
//     chain ([sql.TxFromPgx] → [forwarder] → [Watermill CQRS docs]).
//  4. (*app).incrementCounter — one pgx transaction: load, execute,
//     appendToStream, publish, commit.
//  5. (*app).runForwarder — outbox drainer, BeginnerFromPgx +
//     InitializeSchema.
//  6. ensureStream — manual JetStream stream bootstrap (watermill-nats
//     does not create streams for you).
//  7. (*app).runProjection — cqrs.EventGroupProcessor + onIncremented.
//  8. (*app).incrementHandler / showHandler — Echo handlers and the
//     read-your-writes poll.
//
// See watermill-cookbook.md next to this file for prose commentary on
// each recipe and the gotchas each one protects against.
//
// [Transactional Outbox pattern]: https://microservices.io/patterns/data/transactional-outbox.html
// [Werner Vogels on read-your-writes]: https://www.allthingsdistributed.com/2008/12/eventually_consistent.html
// [sql.TxFromPgx]: https://pkg.go.dev/github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql#TxFromPgx
// [forwarder]: https://pkg.go.dev/github.com/ThreeDotsLabs/watermill/components/forwarder
// [Watermill CQRS docs]: https://watermill.io/docs/cqrs/
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
	"github.com/labstack/echo/v4"
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
// 2. Counter aggregate
//
// Counter is the aggregate, Incremented is its only event. Increment is
// the command (returns an event), Apply folds an event into state.
// time.Now / uuid.New are called inline — this is a demo, not a place
// to hide a Clock port behind an interface.
// ======================================================================

type Counter struct {
	id      string
	value   int64
	version int64
}

type Incremented struct {
	EventID       uuid.UUID `json:"event_id"`
	CounterID     string    `json:"counter_id"`
	Delta         int64     `json:"delta"`
	StreamVersion int64     `json:"stream_version"`
	OccurredAt    time.Time `json:"occurred_at"`
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
// 3. app struct + the three-layer outbox publisher chain
//
// app holds the per-process dependencies that every domain method,
// every projection handler, and every HTTP handler needs. Hanging
// methods off this struct keeps signatures short and stops the
// dependencies from leaking across requests.
//
// newTxEventBus is the core recipe. sql.NewPublisher(sql.TxFromPgx(tx))
// binds the watermill publisher to an already-open pgx transaction, so
// the outbox INSERT rides the same commit/rollback as the domain
// INSERT. forwarder.NewPublisher wraps it in an envelope tagged with
// the destination topic, and cqrs.NewEventBusWithConfig adds
// type-based name routing via cqrs.JSONMarshaler + cqrs.StructName.
//
// See watermill-cookbook.md Recipe 1 (per-tx construction) and
// Recipe 6 (AutoInitializeSchema vs InitializeSchema split) for the
// reasoning behind this exact shape.
// ======================================================================

const (
	outboxTopic = "outbox" // watermill-sql "topic" → physical watermill_outbox table
	eventsTopic = "events" // NATS subject + projection subscribe topic
)

type app struct {
	pool   *pgxpool.Pool
	logger watermill.LoggerAdapter
}

func (a *app) newTxEventBus(tx pgx.Tx) (*cqrs.EventBus, error) {
	sqlPub, err := wsql.NewPublisher(
		wsql.TxFromPgx(tx),
		wsql.PublisherConfig{SchemaAdapter: wsql.DefaultPostgreSQLSchema{}},
		a.logger,
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
		Logger:    a.logger,
	})
}

// ======================================================================
// 4. The write path — four steps inside one transaction
//
//	Begin → { Load → Execute → AppendToStream → Publish } → Commit → Apply
//
// The four bracketed steps run inside the tx. If any of them fails,
// the deferred Rollback undoes everything (including the outbox
// INSERT), so there is no "event persisted but not published" state.
// Apply runs after Commit to advance the in-memory aggregate state
// only when the durable write actually landed.
// ======================================================================

func (a *app) incrementCounter(ctx context.Context, id string, delta int64) (int64, error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	c, err := a.loadCounter(ctx, tx, id)
	if err != nil {
		return 0, err
	}

	e := c.Increment(delta)

	if err := a.appendToStream(ctx, tx, id, e); err != nil {
		return 0, err
	}

	// bus only needs the open tx; building it after appendToStream
	// keeps the happy path linear and saves an unused chain on the
	// error path.
	bus, err := a.newTxEventBus(tx)
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

func (a *app) loadCounter(ctx context.Context, tx pgx.Tx, id string) (*Counter, error) {
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

func (a *app) appendToStream(ctx context.Context, tx pgx.Tx, streamID string, e *Incremented) error {
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

func (a *app) runForwarder(ctx context.Context, downstream message.Publisher) error {
	sub, err := wsql.NewSubscriber(
		wsql.BeginnerFromPgx(a.pool),
		wsql.SubscriberConfig{
			SchemaAdapter:    wsql.DefaultPostgreSQLSchema{},
			OffsetsAdapter:   wsql.DefaultPostgreSQLOffsetsAdapter{},
			InitializeSchema: true,
		},
		a.logger,
	)
	if err != nil {
		return err
	}
	defer sub.Close()

	fwd, err := forwarder.NewForwarder(sub, downstream, a.logger, forwarder.Config{
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
// onIncremented's SQL is a monotonic UPSERT: the WHERE clause makes
// duplicate or out-of-order deliveries silent no-ops, so no separate
// dedup/inbox table is needed.
// ======================================================================

func (a *app) runProjection(ctx context.Context, nc *natsgo.Conn) error {
	router, err := message.NewRouter(message.RouterConfig{}, a.logger)
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
				Logger: a.logger,
			})
		},
		Marshaler: cqrs.JSONMarshaler{GenerateName: cqrs.StructName},
		Logger:    a.logger,
	})
	if err != nil {
		return err
	}

	if err := eventProc.AddHandlersGroup("counter_view",
		cqrs.NewGroupEventHandler(a.onIncremented),
	); err != nil {
		return err
	}

	return router.Run(ctx)
}

func (a *app) onIncremented(ctx context.Context, e *Incremented) error {
	_, err := a.pool.Exec(ctx, `
		INSERT INTO counter_view (counter_id, value, last_event_seq)
		VALUES ($1, $2, $3)
		ON CONFLICT (counter_id) DO UPDATE
		SET value          = counter_view.value + EXCLUDED.value,
		    last_event_seq = EXCLUDED.last_event_seq
		WHERE counter_view.last_event_seq < EXCLUDED.last_event_seq`,
		e.CounterID, e.Delta, e.StreamVersion)
	if err != nil {
		return fmt.Errorf("upsert counter view: %w", err)
	}
	return nil
}

// ======================================================================
// 8. HTTP handlers — Echo
//
// POST /counters/:id/increment  body: {"delta": N}
// GET  /counters/:id[?after=N]
//
// Echo lets each handler return an error or call c.JSON in one
// statement, so all the response-write boilerplate (Content-Type,
// WriteHeader, fmt.Fprintf) goes away. echo.NewHTTPError carries the
// status code and message in a single value.
//
// ?after=N is a read-your-writes ticket: the GET blocks until
// counter_view.last_event_seq reaches at least N, up to a 5-second
// budget. readCounterView handles the polling.
// ======================================================================

var errReadTimeout = errors.New("read-your-writes timeout")

func (a *app) incrementHandler(c echo.Context) error {
	var body struct {
		Delta int64 `json:"delta"`
	}
	if err := c.Bind(&body); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	if body.Delta <= 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "delta must be positive")
	}
	id := c.Param("id")
	version, err := a.incrementCounter(c.Request().Context(), id, body.Delta)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusAccepted, echo.Map{
		"counter_id": id,
		"version":    version,
	})
}

func (a *app) showHandler(c echo.Context) error {
	id := c.Param("id")
	var after int64
	if s := c.QueryParam("after"); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < 0 {
			return echo.NewHTTPError(http.StatusBadRequest, "after must be a non-negative integer")
		}
		after = n
	}

	value, version, err := a.readCounterView(c.Request().Context(), id, after)
	switch {
	case err == nil:
		return c.JSON(http.StatusOK, echo.Map{
			"counter_id": id,
			"value":      value,
			"version":    version,
		})
	case errors.Is(err, errReadTimeout):
		return echo.NewHTTPError(http.StatusRequestTimeout, "read-your-writes timeout")
	case errors.Is(err, pgx.ErrNoRows):
		return echo.NewHTTPError(http.StatusNotFound, "not found")
	default:
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
}

// readCounterView is the read-your-writes poll used by showHandler
// (still part of section 8). With after > 0 it polls counter_view
// until last_event_seq reaches at least after or readBudget expires,
// returning errReadTimeout in the latter case. With after == 0 it
// returns immediately, with pgx.ErrNoRows if the counter has no row.
func (a *app) readCounterView(ctx context.Context, id string, after int64) (value, version int64, err error) {
	const (
		pollInterval = 25 * time.Millisecond
		readBudget   = 5 * time.Second
	)

	query := func() (int64, int64, error) {
		var v, ver int64
		e := a.pool.QueryRow(ctx,
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
// main — wire dependencies into app, start the daemons and HTTP
// server, wait for shutdown
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

	a := &app{
		pool:   pool,
		logger: logger,
	}

	go func() {
		if err := a.runForwarder(ctx, natsPub); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("forwarder: %v", err)
		}
	}()
	go func() {
		if err := a.runProjection(ctx, nc); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("projection: %v", err)
		}
	}()

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.POST("/counters/:id/increment", a.incrementHandler)
	e.GET("/counters/:id", a.showHandler)

	go func() {
		log.Println("listening on :8080")
		if err := e.Start(":8080"); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = e.Shutdown(shutdownCtx)
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
