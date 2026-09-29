---
name: nosql-benchmark-timing
description: Measure NoSQL database latency and throughput honestly - warmup, monotonic clocks, p50/p95/p99 not averages, result checksums, multi-process load generators (Python threads cap at a few thousand ops/s because of the GIL), and the NoSQL-specific traps (search-engine refresh visibility, eventual consistency, tunable write concern / consistency level, one-writer embedded stores, coordinator vs replica timing). Lists the real tools per engine (redis-benchmark/memtier, mongoperf/YCSB, cassandra-stress/nb, Rally for Elasticsearch/OpenSearch, this lab's harness). Use when comparing NoSQL engines, indexes, document shapes, poolers or consistency levels, or when someone quotes a single-run number.
---

# NoSQL benchmark timing

## Non-negotiables (the lab's method)
1. **Warm up, then sample.** Discard ≥2 executions (connection warmup, cache, JIT, index warmup), then ≥10 timed runs;
   report p50 and p95, keep raw samples. `nslab.timing.measure()` records `first_ms` (cold-ish) separately.
2. **Monotonic clock around the whole round trip** (`time.perf_counter_ns()`), never wall-clock.
3. **Checksum the result set** so a "fast" variant is not silently returning fewer rows. Every read op records `rows` +
   `checksum` (`nslab.util.checksum_rows`); a rewrite must keep the checksum. Cross-engine, checksums also verify two
   engines computed the *same* answer (e.g. Redis and Cassandra returning the same product ids for a text search).
4. **Load generators must be multi-process.** Python threads plateau at a few thousand ops/s (GIL). The lab forks one
   process per worker (`nslab.workers`): Redis reached 79 k ops/s at 64 processes; the *embedded* engines peak at
   **1 worker** (LMDB 99 k) and fall with threads — an in-process store needs a compiled multi-core host to scale.
5. **One engine at a time on a shared host.** Two benchmarks steal each other's cores; `scripts/run-all.sh` runs stacks
   sequentially. (The lab's first pass had light contention — re-run serially for publication numbers.)
6. **State the consistency / durability level** — it changes the number by 10×:
   - MongoDB write concern w:0/w:1/w:majority = 0.3/1.5/4.5 ms; read concern linearizable adds a round trip.
   - Cassandra consistency ONE vs LOCAL_QUORUM vs ALL.
   - Redis async vs `WAIT n` (synchronous replica ack).
7. **Search engines are eventually consistent for search.** A just-written doc is invisible until the next `refresh`
   (Elasticsearch default 30 s). Benchmark reads *after* a refresh, and never confuse "index throughput" with
   "searchable throughput". `refresh=wait_for` blocks the write until the cycle; `refresh=true` forces it (27 ms here).
8. **Separate client time from server time** when they disagree: Mongo `explain(executionStats)`, Cassandra request
   tracing, ES Search Profile API / `took`, Redis `SLOWLOG`/`COMMANDLOG`.
9. **Report the environment**: engine version + image tag, CPU/RAM, storage, replication factor, driver + protocol,
   data scale. Store one JSON per run (see `results/<engine>/latest.json`) and derive tables from it — never hand-edit numbers.

## Coordinator vs replica, embedded single-writer
- A wide-column/leaderless read at `LOCAL_QUORUM` touches multiple replicas; at `ONE` it hits one. Say which.
- Embedded stores (LMDB/RocksDB) are single-writer: the harness uses threads and one shared handle. Their "concurrency
  scaling" measures the GIL, not the engine — treat single-thread numbers as the real point-op cost.

## Tools per engine (all real; pick by engine)
| tool | engines | measures | sketch |
|---|---|---|---|
| **nslab** (this repo) | 7+ families | operation matrix, capability probes, optimisation, connection storms, replication lag, failover, load test | `uv run nslab run redis --failover` |
| **redis-benchmark** / **memtier_benchmark** | Redis/Valkey/KeyDB/Dragonfly | ops/s + percentiles per command | `memtier_benchmark -s host -p 6379 --ratio=1:1 -c 50 -t 4` |
| **YCSB** | most (Mongo, Cassandra, Redis, ES, ...) | workloads A–F, throughput/latency | `ycsb run mongodb -P workloads/workloada` |
| **cassandra-stress** / **nosqlbench (nb5)** | Cassandra/Scylla (nb: many) | write/read/mixed, coordinator-side | `cassandra-stress write n=1M -rate threads=64` |
| **mongoperf** / driver bench | MongoDB | disk + op latency | custom driver loop with percentiles |
| **Elasticsearch/OpenSearch Rally** | ES/OpenSearch | indexing + query tracks (official) | `esrally race --track=geonames` |
| **db_bench** | RocksDB (and LMDB via forks) | LSM read/write/compaction | `db_bench --benchmarks=fillrandom,readrandom` |
| **hyperfine** | any CLI (redis-cli/mongosh/cqlsh) | end-to-end command latency with warmup | `hyperfine --warmup 3 'redis-cli GET k'` |

## Files
- `scripts/bench_op.py` — time one catalog operation against a running stack with warmup / percentiles / checksum.
- `references/methodology.md` — the checklist expanded with the NoSQL pitfalls found while building the lab.
