# watermill-practice

Watermill と PostgreSQL、NATS JetStream で組んだ [Transactional Outbox パターン][outbox] の
単一ファイル Go デモ。aggregate → event store → outbox → forwarder → broker →
projection → read model → HTTP のパイプライン全体が `main.go` に収まっていて
（約 590 行、8 セクション）、隣にある `watermill-cookbook.md` がその裏側にある罠と
判断を 6 本のレシピで解説している。

## 実行

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

`?after=N` を付けると projection がバージョン N に追いつくまで GET がブロックする
（タイムアウト 5 秒）。自分の POST で作ったバージョンを確実に読み返せる
[read-your-writes][ryw] の仕組み。

## このリポジトリの位置づけ

スターターテンプレートというより、outbox パターンの形と罠を読み解くための教材。
Watermill で書くとどう短くなるか、どこで詰まりやすいかを実例で示すことに集中していて、
次の話題は意図的に外した: schema evolution / upcaster、saga、dead-letter queue、
multi-aggregate トランザクション、プロダクション向け observability。同じリストは
cookbook の末尾にもある。

[outbox]: https://microservices.io/patterns/data/transactional-outbox.html
[ryw]: https://www.allthingsdistributed.com/2008/12/eventually_consistent.html
