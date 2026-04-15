# Watermill event-sourcing cookbook

Six recipes for using Watermill as the transport layer of an
event-sourced PostgreSQL + NATS JetStream stack, following the
[Transactional Outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html).
Each recipe is demonstrated **runnable** in the single file next to this one,
[`main.go`](./main.go). The two files are all you need: drop them into
any project, adjust the domain types, and the pipeline works.

If you only read one thing, read `main.go`. If you want the *why*
behind its watermill-specific sections (3 through 8), read the
corresponding recipe below — sections 1 and 2 (DDL and aggregate)
are domain glue and have no recipe.

| # | Recipe                                              | Demonstrated in `main.go`                  |
| - | --------------------------------------------------- | ------------------------------------------ |
| 1 | Atomic event-store + outbox write in one pgx tx     | section 3 (`newTxEventBus`) + section 4 (`incrementCounter`) |
| 2 | Outbox drain daemon                                 | section 5 (`runForwarder`)                 |
| 3 | NATS JetStream stream bootstrap                     | section 6 (`ensureStream`)                 |
| 4 | Typed projection loop via `cqrs.EventGroupProcessor`| section 7 (`runProjection`)                |
| 5 | Event name routing via `cqrs.StructName`            | sections 3 & 7 (marshaler config)          |
| 6 | `AutoInitializeSchema` vs `InitializeSchema`        | sections 3 & 5 (commentary)                |

---

## Recipe 1 — Atomic event-store append + outbox publish in one pgx TX

### Problem

If you write an event to an `events` table and then publish it to a bus,
a crash between the two leaves the event persisted but undelivered. If
you publish first and then write, a crash between the two leaves a
delivered event that never happened. Neither is acceptable in an
event-sourced system.

### Solution

Bind Watermill's SQL publisher to an already-open pgx transaction using
[`sql.TxFromPgx(tx)`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql#TxFromPgx),
then wrap it with [`forwarder.NewPublisher`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill/components/forwarder)
and `cqrs.NewEventBusWithConfig`:

```go
sqlPub, _ := wsql.NewPublisher(
    wsql.TxFromPgx(tx),
    wsql.PublisherConfig{SchemaAdapter: wsql.DefaultPostgreSQLSchema{}},
    logger,
)
fwdPub := forwarder.NewPublisher(sqlPub, forwarder.PublisherConfig{
    ForwarderTopic: "outbox",
})
bus, _ := cqrs.NewEventBusWithConfig(fwdPub, cqrs.EventBusConfig{
    GeneratePublishTopic: func(_ cqrs.GenerateEventPublishTopicParams) (string, error) {
        return "events", nil
    },
    Marshaler: cqrs.JSONMarshaler{GenerateName: cqrs.StructName},
    Logger:    logger,
})
```

Now `bus.Publish(ctx, event)` INSERTs into `watermill_outbox` through the
SAME transaction as your domain INSERT. One commit makes both visible;
one rollback removes both.

- `TxFromPgx(tx)` is the critical piece — it's what "joins" the watermill publisher to your transaction.
- `forwarder.NewPublisher` wraps the raw SQL publisher with an envelope
  that tags the destination topic (`"events"`) so the drainer
  (Recipe 2) knows where to republish each row later.
- `cqrs.NewEventBusWithConfig` is optional in theory — you could call
  `fwdPub.Publish("events", msg)` directly — but in practice it's worth
  the two extra config lines because it gives you type-based event
  name routing (Recipe 5) for free. See the [watermill forwarder docs](https://watermill.io/docs/forwarder/)
  and [real-world example](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/real-world-examples/transactional-events-forwarder)
  for the full picture.

### Gotcha: `AutoInitializeSchema` MUST be false

The default is false, which is correct. Do not set it to true on the
publisher side: [`sql.NewPublisher`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql#PublisherConfig)
returns an error if `AutoInitializeSchema: true` is combined with
`TxFromPgx` — schema bootstrap belongs in migrations, not inside a
domain transaction. (On database engines where DDL implicitly commits,
such as MySQL, the schema `CREATE TABLE` would also silently fragment
the atomic write; PostgreSQL does not have this problem — it has
[transactional DDL](https://wiki.postgresql.org/wiki/Transactional_DDL_in_PostgreSQL:_A_Competitive_Analysis)
— but watermill-sql forbids the combination unconditionally because the
framework supports both engines.)

### Gotcha: build the publisher chain per transaction

`sql.Publisher` binds to the tx passed to `TxFromPgx`. You can't cache
one across requests — each HTTP handler, each command, each unit of
work needs its own chain. The construction cost is small (~a handful
of allocations) and this is the pattern Watermill's own
`_examples/real-world-examples/transactional-events-forwarder` sample
uses.

---

## Recipe 2 — Outbox drain daemon

### Problem

The outbox table holds events after commit, but nothing has pushed
them onto the real bus (NATS, Kafka, etc.) yet. You need a long-running
process that polls the outbox, reads each row's envelope, and
republishes to the downstream publisher.

### Solution

`forwarder.NewForwarder` plus a subscriber that reads from the outbox
table. Use [`sql.BeginnerFromPgx(pool)`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql#BeginnerFromPgx)
— NOT `TxFromPgx` — because the drainer opens its own short transactions per poll:

```go
sub, _ := wsql.NewSubscriber(
    wsql.BeginnerFromPgx(pool),
    wsql.SubscriberConfig{
        SchemaAdapter:    wsql.DefaultPostgreSQLSchema{},
        OffsetsAdapter:   wsql.DefaultPostgreSQLOffsetsAdapter{},
        InitializeSchema: true, // ← the one place this must be on
    },
    logger,
)
defer sub.Close()

fwd, _ := forwarder.NewForwarder(sub, downstream, logger, forwarder.Config{
    ForwarderTopic: "outbox",
})
fwd.Run(ctx) // blocks until ctx is canceled
```

### Gotcha: this is where `InitializeSchema: true` lives

`watermill_outbox` and `watermill_offsets_outbox` have a specific shape
that must match `DefaultPostgreSQLSchema` exactly. Rather than
duplicating their DDL in your application migrations, let the
subscriber bootstrap them on first run. This is the one place in the
whole stack where schema auto-init must be enabled. It runs outside
any application transaction, so the implicit commit problem from
Recipe 1 does not apply.

### Gotcha: Beginner vs Tx

[`sql.BeginnerFromPgx`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql#BeginnerFromPgx)
gives watermill a "can begin transactions" handle;
[`sql.TxFromPgx`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql#TxFromPgx)
gives watermill "an already-begun transaction to use".
Subscribers want the former, publishers embedded in a domain
transaction want the latter. Mixing them up gives you a silently
broken drainer.

---

## Recipe 3 — NATS JetStream stream bootstrap

### Problem

You wire up a Watermill JetStream publisher and subscriber, start
traffic, and nothing moves. No error. No log line. Just silence.

### Cause

`watermill-nats` does not create the JetStream stream for you. Both
the publisher and the subscriber expect the stream to already exist
when they attach. If it does not exist, they fail to find it and the
pipeline silently does nothing.

### Solution

Call `CreateOrUpdateStream` at startup, idempotent:

```go
js, _ := natsjs.New(nc)
_, err := js.CreateOrUpdateStream(ctx, natsjs.StreamConfig{
    Name:     "events",
    Subjects: []string{"events"},
})
```

Do this BEFORE constructing any watermill publisher or subscriber that
targets the stream. `CreateOrUpdate` is safe to call on every process
start (see [jetstream.StreamManager](https://pkg.go.dev/github.com/nats-io/nats.go/jetstream#StreamManager)) — it creates if missing and leaves alone if present.

### Why this isn't automatic

[JetStream streams](https://docs.nats.io/nats-concepts/jetstream/streams)
have non-trivial configuration (retention, storage type, replica count,
max age, subject list). Watermill deliberately does not guess; the
framework author pushes the choice back to your startup code. The
trade-off is clarity at the cost of one forgotten line biting every
first-time user.

---

## Recipe 4 — Typed projection loop via `cqrs.EventGroupProcessor`

### Problem

You want a read-model updater that subscribes to the event bus and
dispatches typed handlers (`func(ctx, *Incremented) error`) by event
type, without writing your own message router.

### Solution

[`cqrs.EventGroupProcessor`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill/components/cqrs#EventGroupProcessor)
wires a subscriber, a marshaler, and a group of typed handlers onto a
watermill router (see the [watermill CQRS docs](https://watermill.io/docs/cqrs/)):

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
        // see main.go section 7 for the actual UPSERT
        return nil
    }),
)

router.Run(ctx)
```

`cqrs.NewGroupEventHandler` takes a generic
`func[T any](ctx context.Context, event *T) error`. The router looks at
the `name` metadata field on each incoming message, matches it against
`cqrs.StructName(new(T))`, and dispatches. Add another handler for
another event type to the same group and all of them share one
subscription.

### Gotcha: the marshaler config must match the publish side

If the write side uses `cqrs.StructName` and the read side uses
`cqrs.FullyQualifiedStructName`, the dispatch keys won't match and your
handlers never fire. Recipe 5 explains why.

---

## Recipe 5 — Event name routing via `cqrs.StructName`

### Problem

Multiple event types flow through one subject. The projection has to
know which handler to invoke for each. You don't want to hand-maintain
a string registry that gets out of sync with your Go types.

### Solution

[`cqrs.JSONMarshaler.GenerateName`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill/components/cqrs#JSONMarshaler)
is the single-point-of-truth function that turns a Go struct into a
routing string. Two built-ins ship:

- `cqrs.FullyQualifiedStructName` — returns `"counter.Incremented"`
  (package-qualified).
- [`cqrs.StructName`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill/components/cqrs#StructName)
  — returns `"Incremented"` (trims package prefix).

Pick one, use it on BOTH sides. `cqrs.StructName` is rename-stable
across package moves (at the cost of cross-package name collisions,
which you solve by choosing unique struct names):

```go
Marshaler: cqrs.JSONMarshaler{GenerateName: cqrs.StructName}
```

When `bus.Publish(ctx, &Incremented{...})` runs, the marshaler sets the
message metadata `name = "Incremented"`. When the
`cqrs.EventGroupProcessor` receives it, it calls
`cqrs.StructName(new(Incremented))` → `"Incremented"`, matches, and
dispatches to the handler registered for `*Incremented`. No registry
maintenance.

### Gotcha: package-move-safe but rename-unsafe

`cqrs.StructName` strips the package prefix, so moving `Incremented`
between packages does not change its routing key. Renaming the struct
itself, however, does — `Incremented` → `CounterIncremented` breaks
every stored event with the old name, because the events table
decodes by name and the bus routes by name. Renaming is a protocol
change. If you need to rename, write an upcaster that reads old
events into new structs at load time.

---

## Recipe 6 — `AutoInitializeSchema` vs `InitializeSchema`: the split

> Builds on Recipe 1 (per-tx publisher) and Recipe 2 (drainer).

### Problem

Watermill's SQL package has two schema auto-init flags, one on the
publisher config and one on the subscriber config. They look
symmetrical. They are not. Using them wrong is silent and painful.

### The rule

|                         | Publisher                                 | Subscriber                                         |
| ----------------------- | ----------------------------------------- | -------------------------------------------------- |
| Config field            | [`PublisherConfig.AutoInitializeSchema`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql#PublisherConfig)    | [`SubscriberConfig.InitializeSchema`](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql#SubscriberConfig)                |
| Required value          | **false**                                 | **true** (first run only, idempotent after)        |
| Why                     | `NewPublisher` returns an error if this is true with `TxFromPgx`. Schema bootstrap belongs in migrations. (On MySQL, DDL would also silently commit the open tx; PostgreSQL has [transactional DDL](https://wiki.postgresql.org/wiki/Transactional_DDL_in_PostgreSQL:_A_Competitive_Analysis) and avoids that, but watermill-sql forbids the combination on all engines.) | Subscriber runs outside any domain tx. DDL runs cleanly. |

Every other combination is wrong in some subtle way:

- **Publisher true**: `NewPublisher` returns an error immediately —
  watermill-sql refuses this combination with `TxFromPgx`. You will
  not get a working publisher; every transaction that tries to build
  the publisher chain errors out at the `NewPublisher` call.
- **Subscriber false**: watermill never creates `watermill_outbox`.
  The drainer attaches to a non-existent table and fails to poll.
  Depending on your retry logic, this can manifest as "the forwarder
  is silently stuck" rather than "the forwarder crashed".

### Why the asymmetry

The field names are different for a reason: the publisher's flag
*allows* auto-init but watermill forbids combining it with
`TxFromPgx` (it will return an error from `NewPublisher`). The
subscriber's flag *performs* auto-init unconditionally. The types
encode the safe combination — but only if you read the docs.

### Gotcha: your migrations must NOT declare the outbox tables

Let watermill own `watermill_outbox` and `watermill_offsets_outbox`.
If your migration also creates them, you'll diverge from whatever
shape the installed watermill version expects, and one day
`SchemaAdapter.InsertQuery` will generate SQL that references columns
your migration did not create.

---

## What this cookbook does not cover

- **Event schema evolution** — `cqrs.StructName` is rename-unsafe.
  Real schema migration requires an upcaster.
- **Fan-out to multiple projections** — trivial once Recipe 4 is in
  place: call `AddHandlersGroup` multiple times with different group
  names. They share one subscriber but maintain independent offsets
  through NATS JetStream consumer positions.
- **Dead-letter handling** — Watermill has `middleware.PoisonQueue`;
  this cookbook does not demonstrate it.
- **Saga / process managers** — neither this cookbook nor `main.go`
  has any.
