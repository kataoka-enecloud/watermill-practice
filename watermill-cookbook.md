# Watermill event-sourcing cookbook

## そもそも outbox パターンとは

イベントを「DB に書く」と「バスに届ける」を 1 つの transaction にできれば理想だが、DB とメッセージブローカーは別システムなのでそれはできない。ではどちらを先にするか？ 書いてから publish すると、その間にプロセスが死んだとき「保存済み・未配信」になる。publish してから書くと「配信済み・未保存」になる。どちらも event-sourced システム（イベントの履歴そのものを状態として管理するシステム）では致命的だ。

```go
// 壊れているコード: events に書いてから publish
tx, _ := pool.Begin(ctx)
tx.Exec(ctx, `INSERT INTO events ...`)
tx.Commit(ctx)
// ← ここでクラッシュすると、保存済みなのに配信されない
natsPub.Publish("events", msg)
```

これを救うのが outbox パターン。

outbox パターン（[Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html)）はこう解く。まず「outbox（送信待ちイベントを一時退避するテーブル）」を**同じ DB に**用意する。ドメインの書き込みと outbox への INSERT を同一 transaction に入れる。これで「一緒にコミットされる / 一緒にロールバックされる」が保証できる。その後、別プロセスの forwarder（outbox を読んで実バスへ転送する常駐プロセス）が outbox テーブルをポーリングして NATS 等へ転送する。forwarder はドメイン transaction とは無関係に非同期で動き、at-least-once 配信を保証する。

```
HTTP POST
   │
   ▼
pgx TX ─────────────────────────────────────────────┐
  ├─ events テーブルに INSERT (event store)           │ 同一 commit
  └─ watermill_outbox テーブルに INSERT (outbox)  ───┘
          │
          ▼ (forwarder が非同期で転送)
       NATS JetStream
          │
          ▼
      projection → counter_view (read model)
```

このパターンを Go + Watermill + PostgreSQL + NATS JetStream で実装したのが隣の `main.go` だ。以下 6 つのレシピがその「なぜそうなっているか」を説明する。

---

| # | レシピ | `main.go` での実装箇所 |
| - | ------ | ---------------------- |
| 1 | pgx TX 1 つでイベントストア追記 + outbox publish をアトミックに | section 3 (`newTxEventBus`) + section 4 (`incrementCounter`) |
| 2 | Outbox drain daemon | section 5 (`runForwarder`) |
| 3 | NATS JetStream stream のブートストラップ | section 6 (`ensureStream`) |
| 4 | `cqrs.EventGroupProcessor` による型付き projection ループ | section 7 (`runProjection`) |
| 5 | `cqrs.StructName` によるイベント名ルーティング | sections 3 & 7 (marshaler config) |
| 6 | `AutoInitializeSchema` vs `InitializeSchema` | sections 3 & 5 (commentary) |

---

## Recipe 1 — pgx TX 1 つでイベントストア追記 + outbox publish をアトミックに

**`main.go` section 3 (`newTxEventBus`) + section 4 (`incrementCounter`) で実装している。**

### 問題

`events` テーブルに書いてから publish すると、その間にクラッシュしたとき「保存済み・未配信」になる。逆に先に publish してから書くと「配信済み・未保存」になる。event-sourced システムではどちらも許容できない。

### 解決策

3 つの層を順に組み立てる。

```go
sqlPub, _ := wsql.NewPublisher(
    wsql.TxFromPgx(tx),
    wsql.PublisherConfig{SchemaAdapter: wsql.DefaultPostgreSQLSchema{}},
    logger,
)
```

[`TxFromPgx(tx)`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql#TxFromPgx) がドメイン tx と watermill publisher を縛る。ここだけではまだ「どの topic に書くか」を知らないため、次の層でラップする。

```go
fwdPub := forwarder.NewPublisher(sqlPub, forwarder.PublisherConfig{
    ForwarderTopic: "outbox",
})
```

[`forwarder.NewPublisher`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill/components/forwarder) はメッセージを envelope に包んで outbox 行に書く。転送先 topic（`events`）はその envelope の中に埋め込まれるので、Recipe 2 の drainer が outbox 行を読んだときに「どこへ転送すべきか」が決まる。

```go
bus, _ := cqrs.NewEventBusWithConfig(fwdPub, cqrs.EventBusConfig{
    GeneratePublishTopic: func(_ cqrs.GenerateEventPublishTopicParams) (string, error) {
        return "events", nil
    },
    Marshaler: cqrs.JSONMarshaler{GenerateName: cqrs.StructName},
    Logger:    logger,
})
```

`cqrs.EventBus` は理論上省略可能だ（`fwdPub.Publish("events", msg)` を直接呼んでも動く）。それでも 1 段挟むのは、struct 型から routing key を自動生成させるためで、その仕組みは Recipe 5 で詳説する。詳細は [watermill forwarder docs](https://watermill.io/docs/forwarder/) と [real-world example](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/real-world-examples/transactional-events-forwarder) も参照。

これで `bus.Publish(ctx, event)` は、ドメイン INSERT と**同じ** transaction を通じて `watermill_outbox` に INSERT する。commit 1 回で両方が見え、rollback 1 回で両方が消える。

### 罠: `AutoInitializeSchema` は必ず false

デフォルトは false で、これが正しい。publisher 側で true にしてはいけない。[`sql.NewPublisher`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql#PublisherConfig) は `AutoInitializeSchema: true` と `TxFromPgx` を組み合わせると即座にエラーを返す。schema の bootstrap は migrations に属するものであり、ドメイン transaction の中ではない。（DDL が implicit commit を起こす MySQL では、`CREATE TABLE` がアトミックな書き込みを黙って分断してしまう。PostgreSQL は [transactional DDL](https://wiki.postgresql.org/wiki/Transactional_DDL_in_PostgreSQL:_A_Competitive_Analysis) を持つのでその問題はないが、watermill-sql は両エンジンをサポートするためすべてのエンジンでこの組み合わせを無条件に禁じている。）

### 罠: publisher チェーンは transaction ごとに作る

`sql.Publisher` は `TxFromPgx` に渡した tx に束縛される。リクエストをまたいでキャッシュすることはできない。HTTP ハンドラ 1 回・コマンド 1 回・作業単位 1 回につき、それぞれ独自のチェーンを作る。構築コストはアロケーション数個程度で小さい。Watermill 公式の `_examples/real-world-examples/transactional-events-forwarder` も同じパターンだ。

---

## Recipe 2 — Outbox drain daemon

**`main.go` section 5 (`runForwarder`) で実装している。**

### 問題

outbox テーブルはコミット後にイベントを保持しているが、実際のバス（NATS、Kafka 等）にはまだ push されていない。outbox をポーリングし、各行の envelope を読んで downstream の publisher へ republish する長時間稼働プロセスが必要だ。

### 解決策

`forwarder.NewForwarder` と outbox テーブルを読む subscriber を組み合わせる。subscriber には [`sql.BeginnerFromPgx(pool)`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql#BeginnerFromPgx) を渡す。`TxFromPgx` ではない — drainer はポーリングごとに独自の短い transaction を自分で開くからだ。

```go
sub, _ := wsql.NewSubscriber(
    wsql.BeginnerFromPgx(pool),
    wsql.SubscriberConfig{
        SchemaAdapter:    wsql.DefaultPostgreSQLSchema{},
        OffsetsAdapter:   wsql.DefaultPostgreSQLOffsetsAdapter{},
        InitializeSchema: true, // ← ここだけ true にする
    },
    logger,
)
defer sub.Close()

fwd, _ := forwarder.NewForwarder(sub, downstream, logger, forwarder.Config{
    ForwarderTopic: "outbox",
})
fwd.Run(ctx) // ctx がキャンセルされるまでブロック
```

### 罠: `InitializeSchema: true` はここに置く

`watermill_outbox` と `watermill_offsets_outbox` は `DefaultPostgreSQLSchema` と完全に一致する特定の形状でなければならない。その DDL をアプリのマイグレーションに書き写すのではなく、subscriber の初回起動時に bootstrap させる。スタック全体でスキーマ自動 init を有効にするのはここだけだ。drainer はドメイン transaction とは無関係に動くため、Recipe 1 で禁じた「ドメイン tx の中での schema 作成」という問題が生じない。

`InitializeSchema: true` を忘れると、drainer 起動時に次のエラーが出る。

```
ERROR: relation "watermill_outbox" does not exist (SQLSTATE 42P01)
```

このエラーは subscriber 側で出るため、publisher チェーンは正常に見える点に注意。

### 罠: Beginner vs Tx

[`sql.BeginnerFromPgx`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql#BeginnerFromPgx) は「transaction を開始できる」ハンドルを watermill に渡す。[`sql.TxFromPgx`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql#TxFromPgx) は「既に開始された transaction を使え」と渡す。subscriber は前者、ドメイン transaction に組み込む publisher は後者。混同するとサイレントに壊れた drainer ができあがる。

---

## Recipe 3 — NATS JetStream stream のブートストラップ

**`main.go` section 6 (`ensureStream`) で実装している。**

### 問題

Watermill の JetStream publisher と subscriber を繋いでトラフィックを流し始めた。何も動かない。エラーなし。ログなし。ただの沈黙。

### 原因

`watermill-nats` は JetStream の stream（メッセージを永続化する名前付きの領域）を代わりに作ってくれない。publisher も subscriber も、アタッチ時に stream がすでに存在することを前提とする。存在しなければ黙って何もしない。

### 解決策

起動時に `CreateOrUpdateStream` を呼ぶ。idempotent なので何度呼んでも安全だ。これを watermill の publisher・subscriber を作るより**前に**実行すること。

```go
js, _ := natsjs.New(nc)
_, err := js.CreateOrUpdateStream(ctx, natsjs.StreamConfig{
    Name:     "events",
    Subjects: []string{"events"},
})
```

`CreateOrUpdate` は存在しなければ作成し、存在すれば設定を更新するだけだ（[jetstream.StreamManager](https://pkg.go.dev/github.com/nats-io/nats.go/jetstream#StreamManager) 参照）。

### なぜ自動化されていないのか

[JetStream の stream](https://docs.nats.io/nats-concepts/jetstream/streams) には非自明な設定が多い（retention ポリシー、storage type、replica 数、max age、subject リスト）。Watermill はあえて推測しない。stream の設定はあなたの起動コードに委ねられている。トレードオフは明快さと引き換えに「1 行書き忘れた初回ユーザーが必ず踏む罠」だ。`main.go` では `main()` の中で `ensureStream` を最初に呼んでいる。

---

## Recipe 4 — `cqrs.EventGroupProcessor` による型付き projection ループ

**`main.go` section 7 (`runProjection`) で実装している。**

### 問題

イベントバスを subscribe してイベント型ごとに型付きハンドラ（`func(ctx, *Incremented) error`）へディスパッチし、read model（クエリ用に最適化したビュー）を更新するコンポーネントが欲しい。しかし独自の message router は書きたくない。

### 解決策

[`cqrs.EventGroupProcessor`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill/components/cqrs#EventGroupProcessor)（subscriber・marshaler・型付きハンドラ群を watermill router に接続するコンポーネント）を使う（[watermill CQRS docs](https://watermill.io/docs/cqrs/) 参照）。

```go
router, _ := message.NewRouter(message.RouterConfig{}, logger)

ep, _ := cqrs.NewEventGroupProcessorWithConfig(router, cqrs.EventGroupProcessorConfig{
    GenerateSubscribeTopic: func(_ cqrs.EventGroupProcessorGenerateSubscribeTopicParams) (string, error) {
        return "events", nil
    },
    SubscriberConstructor: func(_ cqrs.EventGroupProcessorSubscriberConstructorParams) (message.Subscriber, error) {
        return wnats.NewSubscriber(wnats.SubscriberConfig{
            Conn:   nc,
            Logger: logger,
        })
    },
    Marshaler: cqrs.JSONMarshaler{GenerateName: cqrs.StructName},
    Logger:    logger,
})

ep.AddHandlersGroup("counter_view",
    cqrs.NewGroupEventHandler(func(ctx context.Context, e *Incremented) error {
        // 実際の UPSERT は main.go section 7 を参照
        return nil
    }),
)

router.Run(ctx)
```

`cqrs.NewGroupEventHandler` はジェネリックな `func[T any](ctx context.Context, event *T) error` を受け取る。router は受信メッセージの `name` メタデータフィールドを `cqrs.StructName(new(T))` と照合してディスパッチする。同じグループに別イベント型のハンドラを追加すれば、すべてが 1 つの subscription を共有する。

`main.go` では `(*app).runProjection` の中でこれを組んでおり、`onIncremented` が `counter_view` テーブルへの UPSERT を担う。UPSERT の WHERE 句 `WHERE counter_view.last_event_seq < EXCLUDED.last_event_seq` が重複排除のしくみで、単調増加する `stream_version` を条件にしているため、同じイベントが再配信されたり順序が逆転したりしても行は更新されず、冪等（idempotent）に動作する。

### 罠: marshaler の設定は publish 側と合わせること

書き込み側が `cqrs.StructName` を使い、読み込み側が `cqrs.FullyQualifiedStructName` を使うと、dispatch キーが一致せずハンドラが一切発火しない。Recipe 5 で詳説する。

---

## Recipe 5 — `cqrs.StructName` によるイベント名ルーティング

**`main.go` section 3 (`newTxEventBus`) と section 7 (`runProjection`) の marshaler config で実装している。**

### 問題

1 つの subject に複数のイベント型が流れる。projection はイベント型ごとにどのハンドラを呼ぶか判断しなければならない。Go の型と乖離しうる文字列 registry を手動で管理したくない。

### 解決策

[`cqrs.JSONMarshaler.GenerateName`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill/components/cqrs#JSONMarshaler)（Go の struct をルーティング文字列に変換する関数）が、イベント名の唯一の定義元になる。2 つの組み込みがある。

- `cqrs.FullyQualifiedStructName` — `"counter.Incremented"` を返す（パッケージ修飾付き）。
- [`cqrs.StructName`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill/components/cqrs#StructName) — `"Incremented"` を返す（パッケージプレフィックスを除去）。

どちらかを選んで、書き込み側・読み込み側の**両方で**使う。

```go
Marshaler: cqrs.JSONMarshaler{GenerateName: cqrs.StructName}
```

`bus.Publish(ctx, &Incremented{...})` が実行されると、marshaler（struct とバイト列を相互に変換する層）はメッセージメタデータに `name = "Incremented"` をセットする。`cqrs.EventGroupProcessor` が受信すると `cqrs.StructName(new(Incremented))` → `"Incremented"` を呼んで照合し、`*Incremented` に登録されたハンドラへディスパッチする。registry の手動管理は不要だ。

`cqrs.StructName` はパッケージ移動に強い。`Incremented` を別パッケージに移しても routing key は変わらない。ただし、異なるパッケージに同名の struct がある場合は衝突する。

```go
// package main
type Incremented struct{ ... }

// package billing
type Incremented struct{ ... }
```

両方とも `cqrs.StructName` で `"Incremented"` になり、routing key が衝突する。`Counter` の handler が `billing` のイベントを掴む可能性が出る。両者を区別したいなら `cqrs.FullyQualifiedStructName` を使うか、struct 名をユニークにする。

### 罠: パッケージ移動には強いが struct のリネームには弱い

struct 自体をリネームすると routing key が変わる。`Incremented` → `CounterIncremented` にした瞬間、events テーブルに保存済みの古い行も、バスに流れている古いメッセージも、すべて「知らない名前のイベント」になる。これはプロトコル変更だ。リネームが必要なら、読み込み時に古い struct を新しい struct に変換する upcaster を書くこと。

---

## Recipe 6 — `AutoInitializeSchema` vs `InitializeSchema`: 非対称の理由

> Recipe 1（per-tx publisher）と Recipe 2（drainer）を前提とする。

**`main.go` section 3 と section 5 のコメントで説明している。**

### 問題

Watermill の SQL パッケージにはスキーマ自動 init フラグが 2 つある。publisher config に 1 つ、subscriber config に 1 つ。対称に見える。対称ではない。誤った使い方はサイレントで痛い。

### ルール

|  | Publisher | Subscriber |
| - | --------- | ---------- |
| Config フィールド | [`PublisherConfig.AutoInitializeSchema`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql#PublisherConfig) | [`SubscriberConfig.InitializeSchema`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql#SubscriberConfig) |
| 必要な値 | **false** | **true**（初回のみ、以降は idempotent） |
| 理由 | `TxFromPgx` との組み合わせで true にすると `NewPublisher` がエラーを返す。schema の bootstrap は migrations に属する。（MySQL では DDL が開いている tx を implicit commit してしまう。PostgreSQL は [transactional DDL](https://wiki.postgresql.org/wiki/Transactional_DDL_in_PostgreSQL:_A_Competitive_Analysis) を持つためその問題はないが、watermill-sql はすべてのエンジンでこの組み合わせを禁じている。） | subscriber はドメイン tx の外で動く。DDL はクリーンに実行される。 |

他のすべての組み合わせは何らかの形で誤りだ。

- **Publisher true**: `NewPublisher` が即座にエラーを返す。動作する publisher は得られない。publisher チェーンを構築しようとするすべての transaction が `NewPublisher` 呼び出しでエラーになる。
- **Subscriber false**: watermill は `watermill_outbox` を作成しない。drainer は存在しないテーブルにアタッチし、ポーリングに失敗する。retry ロジック次第では「forwarder がクラッシュした」ではなく「forwarder がサイレントにスタックしている」として現れる。

### なぜ非対称なのか

publisher のフラグは自動 init を「許可」するが、watermill は `TxFromPgx` との組み合わせを禁じ（`NewPublisher` からエラーを返す）。subscriber のフラグは自動 init を無条件に「実行」する。フィールド名が異なるのはこの意味の差を表している。型の設計がその安全な組み合わせを表している — ドキュメントを読めばの話だが。

### 罠: マイグレーションで outbox テーブルを定義してはいけない

`watermill_outbox` と `watermill_offsets_outbox` は watermill に管理させること。アプリのマイグレーションでもこれらを作成すると、インストールされた watermill バージョンが期待する形状からずれる。そしていつか `SchemaAdapter.InsertQuery` がマイグレーションに存在しないカラムを参照する SQL を生成する。`main.go` の `schemaDDL`（section 1）が意図的にこれらのテーブルを含んでいないのはそのためだ。

---

## このクックブックがカバーしないこと

- **Event schema evolution** — `cqrs.StructName` はリネームに弱い。本格的なスキーマ移行には upcaster が必要だ。
- **複数の projection へのファンアウト** — Recipe 4 さえあれば単純だ。異なるグループ名で `AddHandlersGroup` を複数回呼ぶだけ。1 つの subscriber を共有しつつ、NATS JetStream の consumer 位置を通じて独立した offset を維持する。
- **Dead-letter ハンドリング** — Watermill には `middleware.PoisonQueue` があるが、このクックブックでは扱わない。
- **Saga / process managers** — このクックブックにも `main.go` にも存在しない。
