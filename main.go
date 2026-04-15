// Command watermill-practice は [Transactional Outbox pattern] を Watermill で実装した
// 自己完結型のデモだ。aggregate → event store → outbox → forwarder → NATS →
// projection → read model → HTTP という一連の流れを 1 ファイルで示す。
//
// 起動:
//
//	docker compose up -d         # postgres :5432 + nats :4222
//	go run .                      # HTTP server on :8080
//
// 動かし方:
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
// ?after=N を指定すると、counter_view.last_event_seq が N 以上になるまで
// GET がブロックする（最大 5 秒）。自分が POST で作ったバージョンを
// 確実に読み返すための read-your-writes 動作だ（[Werner Vogels on read-your-writes] 参照）。
//
// このファイルの 8 つのセクション:
//
//  1. Event store + read model DDL。
//  2. Counter aggregate。
//  3. app struct + (*app).newTxEventBus — 3 層の publisher チェーン
//     （[sql.TxFromPgx] → [forwarder] → [Watermill CQRS docs]）。
//  4. (*app).incrementCounter — 1 つの pgx transaction で load・append・publish・commit。
//  5. (*app).runForwarder — outbox drainer。BeginnerFromPgx + InitializeSchema。
//  6. ensureStream — JetStream stream の手動 bootstrap。
//  7. (*app).runProjection — cqrs.EventGroupProcessor + onIncremented。
//  8. (*app).incrementHandler / showHandler — Echo handlers と read-your-writes poll。
//
// 各レシピの詳細と gotcha は隣の watermill-cookbook.md を参照。
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

	// watermill コアとアダプタ。wmjs / wsql は各ブローカーを
	// watermill の Publisher/Subscriber interface でラップする。
	"github.com/ThreeDotsLabs/watermill"
	wmjs "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/jetstream"
	wsql "github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/components/forwarder"
	"github.com/ThreeDotsLabs/watermill/message"

	// 生ドライバ。jetsdk は公式 NATS SDK の JetStream サブパッケージで、
	// ensureStream での stream bootstrap に直接使う。
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
// CREATE IF NOT EXISTS により、初回起動でも再起動でも idempotent に動く。
// watermill_outbox / watermill_offsets_outbox はここに書かない。
// それらは section 5 の subscriber が bootstrap する。
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
// Counter が aggregate、Incremented が唯一のイベント。
// Increment はコマンド（イベントを返す）、Apply はイベントを状態に畳み込む。
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
// 3. app struct + 3 層の outbox publisher チェーン
//
// app はプロセス全体で共有する依存をまとめる struct だ。
// method としてぶら下げることでシグネチャを短く保つ。
//
// newTxEventBus がこのデモの肝だ。[sql.TxFromPgx] で Watermill publisher を
// 既に開いた pgx transaction に紐付け、[forwarder] で envelope にラップし、
// [Watermill CQRS docs] の cqrs.NewEventBusWithConfig で型ベースのルーティングを加える。
// outbox INSERT はドメイン INSERT と同じ commit/rollback に乗る。
// ======================================================================

const (
	outboxTopic = "outbox" // watermill-sql の "topic" → 実テーブル watermill_outbox
	eventsTopic = "events" // NATS subject かつ projection の subscribe topic
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
// 4. 書き込みパス — 1 つの transaction の中の 4 ステップ
//
//	Begin → { Load → Execute → AppendToStream → Publish } → Commit → Apply
//
// {} 内のどこかで失敗すれば deferred rollback がすべてを巻き戻す。
// outbox INSERT も一緒に消えるため「保存済み・未配信」状態にならない。
// Apply は Commit 後に呼ぶ — 永続化が確定してからインメモリ状態を進める。
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

	// appendToStream の後に構築することで正常系を上から下に読めるようにする。
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
// 5. forwarder daemon — watermill_outbox を NATS へ drain する
//
// BeginnerFromPgx を使う（TxFromPgx ではない）。drainer はポーリングごとに
// 自分で短い transaction を開くため、既存の tx に乗る必要がない。
//
// InitializeSchema: true はここだけ有効にする。watermill_outbox と
// watermill_offsets_outbox の DDL は watermill に管理させる。
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
// watermill-nats は stream を自動作成しない。このステップを省くと
// publisher も subscriber も黙って失敗し、パイプラインが止まる。
// CreateOrUpdateStream は idempotent なので起動時に必ず呼ぶ。
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
// 7. projection ループ — NATS → counter_view UPSERT
//
// cqrs.NewGroupEventHandler は型付き func(ctx, *T) error を受け取り、
// メッセージの "name" メタデータでディスパッチする。section 3 の marshaler と
// 同じ cqrs.StructName を使うため、別途 registry は不要だ。
//
// onIncremented の UPSERT は WHERE で stream_version の単調増加を条件にする。
// 重複・順序逆転の配信は idempotent に無視される。
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
// ?after=N を指定すると、counter_view.last_event_seq が N 以上になるまで
// GET がブロックする（最大 5 秒）。自分が書いたバージョンが読めることを保証する
// read-your-writes 動作。
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

// readCounterView は showHandler が使う read-your-writes poll（section 8）。
// after > 0 のとき、last_event_seq が after 以上になるか readBudget が切れるまで
// counter_view をポーリングする。after == 0 のときは即時に返す。
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
		// pgx.ErrNoRows は projection がまだ行を作っていない状態を意味する。
		// version 0 として扱い、ポーリングを続ける。
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
// main — 依存を app に組み込み、daemon と HTTP server を起動してシャットダウンを待つ
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
