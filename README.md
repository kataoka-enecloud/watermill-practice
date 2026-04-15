# watermill-practice

Single-file Go demo of the [Transactional Outbox pattern][outbox] with
Watermill, PostgreSQL, and NATS JetStream. The whole pipeline —
aggregate → event store → outbox → forwarder → broker → projection →
read model → HTTP — lives in `main.go` (~590 lines, eight numbered
sections). `watermill-cookbook.md` next to it is six prose recipes
explaining the gotchas the Watermill docs gloss over. Read the
cookbook for the *why*.

## Run

```
docker compose up -d   # postgres :5432 + nats :4222
go run .               # HTTP server on :8080
```

```
$ curl -X POST localhost:8080/counters/c1/increment -d '{"delta":5}'
{"counter_id":"c1","version":1}

$ curl -X POST localhost:8080/counters/c1/increment -d '{"delta":3}'
{"counter_id":"c1","version":2}

$ curl 'localhost:8080/counters/c1?after=2'
{"counter_id":"c1","value":8,"version":2}
```

`?after=N` is a [read-your-writes ticket][ryw]: the GET blocks until
the projection catches up to version N (5-second budget).

## Stack

Go 1.25 · Watermill v1.5 (sql/v4 + nats/v2 + cqrs + forwarder) ·
pgx/v5 · Echo v4 · PostgreSQL 16 · NATS 2.10 with JetStream.

## What this is and isn't

A teaching artifact, not a starter template. It shows one correct
shape of the outbox pattern with Watermill and the traps around it.
Deliberately omitted: schema evolution / upcasters, sagas, dead-letter
queues, multi-aggregate transactions, production observability. The
cookbook's closing section lists the same.

[outbox]: https://microservices.io/patterns/data/transactional-outbox.html
[ryw]: https://www.allthingsdistributed.com/2008/12/eventually_consistent.html
