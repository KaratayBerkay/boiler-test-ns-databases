---
name: nosql-indexing-optimization
description: Make NoSQL reads and writes fast - the measured wins from the lab's optimisation phase across Redis, MongoDB, Cassandra, Neo4j, Elasticsearch and embedded KV. Covers "a missing access path is a full scan", secondary/composite index and query-table design, keyset vs offset pagination, embedding vs referencing, write batching, batched reads (mget/$in/pipelining), connection pooling, and consistency/durability as a latency lever. Use when a NoSQL query or write is slow, when choosing an index or document shape, or when reviewing NoSQL data-access code.
---

# NoSQL indexing & optimisation

Every number below is best-variant-vs-first-variant p50 from `results/<engine>/latest.json` (optimize phase),
one 28-core host, scale-1.

## 1. A missing access path is a full scan (the biggest lever by far)
Range query on `events(event_type, occurred_at)` with no index vs with one:
- Redis ×6 800 (SCAN + client filter → sorted-set range), RocksDB ×10 600, LMDB ×13 000 (keyspace scan → ordered index key)
- Cassandra ×740 (`ALLOW FILTERING` 4.8 s → denormalised query table `((event_type,month),occurred_at)` 6.5 ms)
- MongoDB ×106, Neo4j ×6.3
- Elasticsearch: every field is indexed at mapping time, so there is nothing to add — but that is why writes cost more.

**Do:** for every query in the app, make sure a native index / query table / index keyspace / relationship serves it.
On KV and embedded stores *you* maintain the ordered index on write; there is no optimiser to save you.

**Composite/partition order matters:** put equality columns before the range column (Cassandra clustering, MongoDB ESR
rule: Equality, Sort, Range). Cassandra SAI on a *low-cardinality* column (`event_type`, 7 values) + a range over ~80k
rows fails with `ReadFailure` — a denormalised query table is the right tool there, not a global index.

## 2. Keyset beats offset for deep pages
Offset/skip walks and discards; keyset / `search_after` / clustering-range seeks straight in. offset→keyset speed-up:
LMDB ×14, RocksDB ×11, MongoDB ×6.3, Elasticsearch ×5.3, Neo4j ×2.2. Cassandra has **no OFFSET** (page-and-discard);
its tuple keyset `(ordered_at,id) > (?,?)` is 2.7 ms vs 77 ms. Never paginate a deep list with skip/offset.

## 3. Batch everything that crosses the network
- **Writes**: 2000 one-by-one vs two 1000-doc bulk calls — MongoDB ×86, Elasticsearch ×66 (`_bulk`), Neo4j ×35 (`UNWIND`),
  Redis ×6.7 (pipeline), Cassandra ×5.4 (`execute_concurrent`). Row-by-row writes are the classic NoSQL mistake.
- **Reads**: 100 sequential GETs vs one batched read — MongoDB ×39 (`$in`), Elasticsearch ×18 (`_mget`), Neo4j ×17,
  Redis ×14 (`MGET`/pipeline), Cassandra ×7 (100 concurrent single-partition reads beat a 100-key `IN`, which serialises
  in the coordinator).

## 4. Pool connections
Fresh connection per operation vs one reused client: Cassandra ×24, Redis ×11, MongoDB ×9.8, Neo4j ×3.4, Elasticsearch
×2.5. The handshake (TCP + auth + topology/prepared-statement discovery) dwarfs a point read. Cassandra connect alone is
~30 ms. Always use a driver connection pool; never open per request.

## 5. Document shape: embed what you read together
`opt06` (order with items): embedded ~2× faster than referenced on Redis and Cassandra, equal on MongoDB. Embed 1:few,
read-mostly children; reference 1:many, write-heavy or independently queried children (avoids unbounded documents and
rewrite-the-whole-doc updates).

## 6. Consistency / durability is a latency dial, not a constant
- **MongoDB write concern**: w:0 0.3 ms, w:1 1.5 ms, w:1+journal 2.2 ms, w:majority 4.5 ms. Read concern: local/majority
  ~0.7 ms, linearizable 3.5 ms (extra round trip). Pick per operation.
- **Redis**: `WAIT 1 <ms>` turns an async write into a synchronous replica ack (~10–30 ms here).
- **Cassandra**: consistency level per statement (ONE < LOCAL_QUORUM < ALL).
- **Elasticsearch**: `refresh=false` 4 ms, `refresh=true` 27 ms (forces a refresh), `refresh=wait_for` = until the next
  refresh cycle (30 s here). Don't force a refresh per write.

## 7. Never do these
- `COUNT(*)` / full scan on a distributed store: Cassandra `scan_full` 17.8 s. Keep a counter or use metadata (ES 5 ms,
  Neo4j 3 ms keep maintained counts).
- Client-side group-by on a store without server aggregation (KV/wide-column/embedded scan 1.7–4.9 s) as a hot path —
  precompute a rollup or use a search/OLAP engine.
- Row-by-row inserts, offset pagination, and one connection per request (see above).

Reproduce any of these: `uv run --project harness nslab phase <stack> optimize` (the experiments and both measurements
are stored per engine).
