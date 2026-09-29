---
name: nosql-connection-consistency
description: Handle NoSQL connections and correctness under concurrency - connection pooling and connect cost, max-connection limits and connection storms, tunable consistency (write concern / read preference / consistency level / WAIT), compare-and-set and optimistic concurrency, what "transaction" means per engine, and retry/idempotency. Measured across Redis, MongoDB, Cassandra, Neo4j, Elasticsearch and embedded KV. Use when configuring a NoSQL client/pool, choosing a consistency or durability level, implementing atomic updates or retries, or debugging lost updates, NotPrimary errors, or connection exhaustion.
---

# NoSQL connections & consistency

## Connection cost and why you must pool
Connect p50: embedded 0 ms (in-process), Redis 2.2 ms, MongoDB 3.9 ms, Elasticsearch 4.2 ms (keep-alive HTTP),
Neo4j 5.9 ms (Bolt handshake), **Cassandra 30 ms** (control connection + prepared-statement/topology metadata). A fresh
connection per operation is 2.5–24× slower than a pooled one (optimisation phase). Rules:
- One driver-managed pool per process; size it to the server's limit, not to your worker count.
- Cassandra/Bolt: reuse prepared statements and the session — most of the 30 ms is one-time metadata.
- Serverless/function runtimes: keep the pool in module scope across invocations.

## Limits and connection storms
Max connections: Redis 10 000, MongoDB 5 000 (set via `--maxConns` / `net.maxIncomingConnections`), Neo4j 800 (bolt
thread pool). The connection-storm test opens N clients at once and records failures and error codes — a pool that opens
more than the server allows gets refused, not queued. Set client pool max ≤ server max, and put a pooler/proxy in front
if many app instances share one server.

## Consistency & durability is per-operation, not global
| engine | write side | read side |
|---|---|---|
| MongoDB | write concern w:0 (0.3ms) / w:1 (1.5) / w:1+j (2.2) / w:majority (4.5) | read concern local/majority (~0.7ms) / linearizable (3.5) ; readPreference primary/secondary/nearest |
| Cassandra | consistency ONE / LOCAL_QUORUM / QUORUM / ALL per statement | same; SERIAL for LWT reads |
| Redis | async by default; `WAIT n <ms>` for synchronous replica ack (~10–30ms) | replica reads are stale until replicated |
| Elasticsearch | `wait_for_active_shards`; `refresh=false/true/wait_for` controls searchability | reads hit any shard copy; `?preference=` pins routing |
| Neo4j | single-primary transactional write | reads consistent within a tx snapshot |
| embedded | single writer, durable batch/txn | consistent snapshot (MVCC) |

Choose the weakest level that is still correct for the operation. Use majority/quorum + journaling only for money-like
writes; use w:1 / ONE / async for logs, metrics, cache.

## Atomic updates: compare-and-set / optimistic concurrency
The lab races 4 threads doing CAS on one document and checks the version advanced exactly once per accepted write:
- **Correct, atomic**: Redis (Lua / `WATCH`/`MULTI`/`EXEC`, or Valkey 9 `SET IFEQ`), MongoDB (`update` filtered on the
  version field + `$inc`), Cassandra (LWT `UPDATE ... IF version = ?`), Neo4j (read version, guarded `SET` in one tx),
  embedded (read-modify-write under the single writer).
- **Elasticsearch**: `if_seq_no` + `if_primary_term` correctly *rejects* a conflicting write (retry with the new seq_no),
  but ES is a search index — do not use it as a linearizable counter or system of record.
Always: read version → conditional write → on rejection, re-read and retry (bounded). Never read-then-write without the
condition; that is the classic lost update (a plain `HINCRBY`/`$inc`/counter is atomic and needs no CAS loop).

## What "transaction" means per engine
- MongoDB replica-set / Neo4j: real multi-document ACID with rollback (`abort`/`rollback`).
- Cassandra: only a **single-partition** conditional `BATCH` is atomic+isolated; a multi-partition `LOGGED BATCH` is
  atomic (all-or-nothing) but **not isolated** and has no rollback.
- Redis `MULTI/EXEC`: atomic and isolated as a unit, but a command that errors mid-batch does **not** roll back the rest.
- Elasticsearch: single-document only. Embedded: one write batch/txn is atomic and durable.

## Retry & idempotency
Retryable: timeouts, `NotWritablePrimary`/`NotPrimary` (10107) after a failover, `Unavailable`/`WriteTimeout` at quorum,
409/version conflicts. Make writes idempotent (deterministic ids, upserts, CAS on version) so a retry after an ambiguous
timeout cannot double-apply. During failover a client sees write errors for the downtime window (Redis Sentinel ~2.5 s,
MongoDB election ~tens of seconds unless tuned) — retry with backoff, do not surface immediately.

Reproduce: `uv run --project harness nslab phase <stack> connections` (connect latency, storms, concurrency scaling,
pool saturation, CAS contention, proxy read/write split).
