# Findings (verified in this lab, Sept 2026; p50 latencies unless stated)

Cross-engine tables: `results/SUMMARY.md`; interactive: `docs/report.html`; raw per-engine data: `results/<engine>/latest.json`;
backup & logging strategy write-up: `docs/backup-logging.md`.
Engines measured: Redis 8, MongoDB 7.0, Cassandra 5.0, Neo4j 5 Community, Elasticsearch 9, LMDB, RocksDB. One 28-core host;
the first pass had light cross-engine CPU contention (re-run serially for publication-grade numbers).

## The shape of a NoSQL benchmark
There is no shared query language, so the lab defines a portable **operation catalog** (28 operations) and each engine
adapter implements the ones it can. "Unsupported" (n/a) is a first-class result and often the most interesting cell: it
says *this data-access pattern has no native expression here — you would do it in application code or a second system.*

## Point operations (what each family is built for)
- **Point read by key** p50: RocksDB 0.03 ms, LMDB 0.02 ms (in-process, no network), Redis 0.21 ms, MongoDB 0.79 ms,
  Cassandra 1.77 ms, Neo4j 2.65 ms, Elasticsearch 2.78 ms. A search engine paying ~3 ms for a primary-key GET is the point:
  it is optimised for inverted-index queries, not KV lookups.
- **Single write (insert)**: RocksDB 0.08 ms, LMDB 0.07 ms, Redis 0.30 ms, Cassandra 1.25 ms (commit-log append, no read),
  MongoDB 3.96 ms (journaled w:1), Elasticsearch 5.16 ms (index a document), Neo4j 5.92 ms (transactional graph write).
- **Server-side atomic increment**: Redis 0.24 ms (`HINCRBY`), Cassandra 3.38 ms (counter column), Neo4j 3.79 ms,
  MongoDB 4.92 ms (`$inc`), Elasticsearch 8.46 ms (painless `_update`). Embedded stores do read-modify-write under the
  single-writer lock (~0.03 ms).

## Compare-and-set under contention (4 threads racing the same document)
The connection phase runs `applied + rejected` CAS loops and checks the version advanced exactly `applied` times:
- **Atomic** (every accepted write advanced the version once): Redis (Lua/`WATCH`), MongoDB (`update` filtered on version),
  Cassandra (LWT `IF version = ?`), Neo4j (guarded `SET` in a tx), LMDB/RocksDB (single-writer).
- **Not atomic**: Elasticsearch with `if_seq_no`/`if_primary_term` **rejected** conflicting writes (optimistic concurrency
  works) but the applied/version bookkeeping shows it is last-write-style, not a linearizable counter — treat ES as a
  search index, not a system of record. (Embedded "not atomic" flags are an artefact of the single-writer serialisation,
  not a correctness problem.)

## Indexing is the biggest lever (optimisation phase, best variant vs unindexed first variant)
- **A missing access path is a full scan.** Range query on `events(event_type, occurred_at)` with no index vs an index:
  Redis ×6 800 (SCAN+filter → sorted-set range), RocksDB ×10 600 / LMDB ×13 000 (keyspace scan → ordered index key),
  Cassandra ×740 (`ALLOW FILTERING` 4.8 s → denormalised query table 6.5 ms), MongoDB ×106, Neo4j ×6.3. Elasticsearch
  indexes every field at mapping time, so "add the missing index" does not apply.
- **Deep pagination**: offset/skip walks and discards; keyset/`search_after`/clustering-range seeks. offset→keyset
  speed-up: LMDB ×14, RocksDB ×11, MongoDB ×6.3, Elasticsearch ×5.3, Neo4j ×2.2. Cassandra has no OFFSET at all
  (page-and-discard); its keyset via `(ordered_at, id) > (?, ?)` is 2.7 ms vs 77 ms discarding.
- **Batching writes**: 2000 one-by-one vs two 1000-doc bulk requests: MongoDB ×86, Elasticsearch ×66, Neo4j ×35 (`UNWIND`),
  Redis ×6.7 (pipeline), Cassandra ×5.4 (`execute_concurrent`). Never write to any of these one document per round trip.
- **Batched reads**: 100 sequential GETs vs one multi-get: MongoDB ×39 (`$in`), Elasticsearch ×18 (`_mget`),
  Neo4j ×17, Redis ×14 (pipeline/`MGET`), Cassandra ×7 (100 concurrent single-partition reads beat a 100-key `IN`).
- **Connection reuse**: a fresh connection per operation vs one pooled client: Cassandra ×24, Redis ×11, MongoDB ×9.8,
  Neo4j ×3.4, Elasticsearch ×2.5. Handshakes (TCP + auth + topology discovery) dwarf a point read — always pool.

## Where each family wins and loses on the operation matrix
- **Aggregation** (`GROUP BY shipping_country` over 200k orders): Elasticsearch 7 ms (aggregations framework),
  Neo4j 202 ms, MongoDB 251 ms (pipeline); Cassandra 4.9 s and Redis/RocksDB/LMDB 1.7–2.5 s all **scan client-side** —
  none has server-side grouping over a non-key field.
- **Full-text search**: Redis 1.6 ms and Elasticsearch 7.4 ms (real inverted indexes: RediSearch, Lucene), MongoDB 11 ms
  (`$text`), Neo4j 17 ms (Lucene full-text index); Cassandra 227 ms and LMDB/RocksDB ~50 ms **scan descriptions**
  (Cassandra 5.0.9 SAI has no analyzed-text option; that arrives in 5.1).
- **Geo radius**: Redis 1.1 ms and MongoDB 2.0 ms (2dsphere) and Elasticsearch 15 ms and Neo4j 18 ms (point index);
  Cassandra 657 ms and LMDB/RocksDB ~160 ms compute **haversine over every row**.
- **Vector kNN** (8-dim, cosine, top-10): Redis 0.84 ms, Neo4j 3.2 ms, Cassandra 4.8 ms (all HNSW), Elasticsearch 9.5 ms;
  MongoDB Community is **n/a** (`$vectorSearch` is Atlas/mongot-only); LMDB/RocksDB brute-force ~55 ms.
- **Graph traversal** (category ancestor chain) and **join** (orders + customer name): Neo4j answers both natively over
  relationships (1.8–2.0 ms); the others chase references (embedded stores fastest at ~0.03 ms because it is local, but
  they cannot express arbitrary traversal — the ancestor walk is hard-coded application code).
- **`scan_full`** (count 1M events): Neo4j 2.9 ms and Elasticsearch 5.3 ms keep a maintained count; MongoDB 578 ms,
  Redis 1.2 s (SCAN), RocksDB 1.0 s, LMDB 747 ms iterate; Cassandra 17.8 s (`COUNT(*)` is a cluster-wide read — never do it).

## Connections & concurrency (forked multi-process workers; point reads)
- **Connect cost** p50: LMDB/RocksDB 0 (in-process), Redis 2.2 ms, MongoDB 3.9 ms, Elasticsearch 4.2 ms (keep-alive HTTP),
  Neo4j 5.9 ms (Bolt handshake), Cassandra 30 ms (control connection + prepared-statement metadata).
- **Throughput ceiling** (64 worker processes): Redis 79 k ops/s, RocksDB 34 k, MongoDB 31 k, Cassandra 16 k, Neo4j 15 k,
  Elasticsearch 13 k. Embedded engines peak at **1 worker** (LMDB 99 k, RocksDB 74 k) and *fall* with more Python threads —
  the GIL serialises in-process calls; a real embedded workload uses one thread per core in a compiled host language.
- **Max connections**: Redis 10 000, MongoDB 5 000, Neo4j 800 (bolt thread pool); Cassandra/ES/embedded unbounded or n/a.

## Replication & failover
- **Redis** (async primary/replica + 3 Sentinels): visibility lag p50 0.74 ms, replica **rejects** writes (`READONLY`),
  Sentinel failover downtime **2.5 s** after `kill`.
- **MongoDB** (3-member replica set): visibility lag p50 1.35 ms, secondaries **reject** writes (`NotWritablePrimary` 10107),
  automatic election downtime **30.5 s** here (default `electionTimeoutMillis` plus the driver's server-selection retry loop;
  tune `settings.electionTimeoutMillis` / priorities to shrink it).
- **Cassandra** (leaderless RF=3): every node **accepts** writes (no primary), so `kill` a node and writes continue —
  first successful write via another node **0.9 s**; read scaling was ×0.73 because pinning the client to one node and
  adding contended nodes did not help point-read throughput on a single host.
- **Elasticsearch** (3 nodes, every index 1 shard + 2 replicas): visibility lag on a replica p50 5-6 ms (realtime GET after
  the synchronous replica write), **every node accepts writes** (any node coordinates), catch-up after a 20k burst
  29-51 ms, read scaling ×1.24 with three copies of every shard. Killing the elected **master** (es3): first successful
  write via another node after **4.9 s** (election + primary-shard promotion; the cluster runs yellow with the dead
  node's replica shards unassigned until it returns). The price of replicas=2 shows in writes: insert 5.2 → 7.1 ms,
  bulk load 217k → 112k docs/s.
- **Neo4j Enterprise cluster** (3 primaries, Raft; `neo4j://` routing for writes): follower visibility lag p50 7-8 ms,
  followers **reject** direct writes (`WriteServiceUnavailable`), catch-up ~18 ms, read scaling ×1.24. Killing the
  **Raft leader**: **20.9 s** until a write succeeded through a surviving routing entry point (leader election + the
  driver refreshing its routing table). Writes cost more than Community (insert 5.9 → 10.3 ms: every commit is a Raft
  round); reads are unchanged.
- **Neo4j Community / LMDB / RocksDB**: single-node (Community cannot replicate; embedded stores replicate externally).

## Bulk-insert load test (1000-doc batches, multi-process; best docs/s)
Elasticsearch 217 k (16 procs, `_bulk`), MongoDB 156 k (32, `insert_many`), Redis 90 k (32, pipeline), Cassandra 34 k
(32, `execute_concurrent`), Neo4j 15 k (32, `UNWIND` — every write is a graph transaction). Embedded engines: n/a
(single process; see the batching optimisation experiment for their write throughput).

## Backup & restore (backup phase: one full backup while point reads run, mutate, restore, verify)
- **Online backups did not move point-read latency**: Redis `BGSAVE` (fork 22 ms) 0.59 vs 0.54 ms p50, MongoDB
  `mongodump --oplog` 0.95 vs 1.01, Cassandra `nodetool snapshot` 1.42 vs 1.63, Elasticsearch `_snapshot` 2.53 vs 2.50,
  LMDB `env.copy` 0.07 vs 0.06, RocksDB `Checkpoint` 0.06 vs 0.09. Neo4j Community has no online option: 34 s of
  downtime per `neo4j-admin database dump` (`STOP DATABASE` is rejected in CE).
- **Neo4j Enterprise vs Community backup**: the Enterprise cluster backs up **online** (`neo4j-admin database backup`
  over the backup port, 20 s, 92 MB, reads unaffected) where Community needs 34 s of downtime; the cluster restore is a
  cluster-wide re-seed (`DROP DATABASE` + `CREATE DATABASE ... seedURI:'file://...'`, 16 s with the database offline,
  every primary seeding from the shared file) and survives `down -v` (DR drill verified, 19 s on a fresh cluster).
- **Restore is where the architectures differ**: storage-format artifacts come back in seconds (Elasticsearch 4.4 s,
  RocksDB 0.1 s, Cassandra 21 s for 45 `nodetool import` calls); logical dumps re-ingest and re-index (MongoDB 60 s for
  1.73M docs); file-at-start-up engines take the service down for the load (Redis **11.4 s** incl. the replica's
  full resync, Neo4j **42 s**). All seven restores were **verified** (counts, deleted docs back, post-backup writes gone).
- **Artifact sizes**: mongodump gzip 51 MB for 188 MB on disk; Neo4j dump 306 MB for a 1.84 GB store; Redis RDB 336 MB
  for 590 MB in memory; Cassandra and ES artifacts equal their (compressed) files. Incremental second backups: ES 8.6 KB
  and Cassandra 10.5 KB for 10 new docs; MongoDB/Redis/Neo4j CE/LMDB are full copies (PBM, Enterprise, BackupEngine
  exist for the first three).
- **Traps that each caused a failed or empty restore first**: an AOF-enabled Redis ignores `dump.rdb` (install it as the
  multi-part AOF base + manifest); a restore stop under Sentinel `down-after 2 s` fails over and syncs the old data back
  (raise `down-after` on all Sentinels first); `mongorestore --oplogReplay` refuses namespace filters (and `--drop` does
  not remove collections created after the backup — `dropDatabase` first); Cassandra 5.0 `sstableloader` cannot parse
  `vector<float, 8>` (use `nodetool import --copy-data` — plain `-c` is `--no-invalidate-caches` and the import *moves*
  the export away); ES `destructive_requires_name` rejects `DELETE lab_*`; every fresh `/backups` volume is root-owned.
  Details: `docs/backup-logging.md`.

## Logging (logging phase: driver/rotation, server log, slow-query + audit round trips, run-time level, volume)
- **Slow-query logs verified on every server engine** (fast op not logged, slow op logged with a duration): Redis
  `SLOWLOG` (KEYS 226 → 221 ms), MongoDB profiler + JSON "Slow query" (1099 → 1097 ms, `COLLSCAN`), Cassandra debug.log
  via MonitoringTask (4.2 s `COUNT(*)` → `<SELECT * FROM lab.orders ALLOW FILTERING>, time 1736 msec` — it logs the
  internal read command, per range, with the yaml-only 500 ms threshold), Elasticsearch per-index slowlog (249 → 197 ms
  ECS JSON line with the query source — on the 3-node cluster the line is written by the data node that ran the
  shard query, so read every member's log), Neo4j **query.log stays empty in Community** even for a 3 s query although
  `db.logs.query.*` are accepted — the in-memory query collector (`db.stats.retrieve('QUERIES')`, 925 → 723 ms) is the
  CE alternative; on the **Enterprise cluster query.log works** (361 → 359 ms logged, threshold changed at run time with
  `dbms.setConfigValue`) and `security.log` records the failed login.
- **Audit logs in open source: Cassandra and Redis only.** `nodetool enableauditlog` → `auditlogviewer` showed the three
  statements with user/source/type/category/ks/scope; Redis `ACL LOG` recorded the failed AUTH and the denied SET.
  MongoDB (Enterprise/PSMDB), Neo4j (Enterprise security.log) and Elasticsearch (needs security) do not.
- **JSON server logs out of the box**: MongoDB and Elasticsearch (ECS). Redis and Cassandra are text; Neo4j is log4j2
  text with a JSON layout available.
- **Run-time log level works everywhere but embedded stores**: `CONFIG SET loglevel`, `logComponentVerbosity`,
  `nodetool setlogginglevel`, `logger.*` cluster settings, and for Neo4j CE the log4j2 `monitorInterval=30` config
  reload (DEBUG lines appeared 21 s after editing `server-logs.xml`; `dbms.setConfigValue` does not exist in CE).
- **Log volume over the lab phases**: MongoDB ~8 MB per member (JSON), Cassandra 1-2 MB per node, ES 236 KB, Redis
  50 KB, Neo4j 7 KB on stdout (its logs are files under /logs). Every stack now rotates at the container level
  (`x-logging`: json-file 50 MB × 3, compressed).

## On Kubernetes (k3s via k3d + Harbor, `docs/kubernetes.md`)
- Every stack, every phase, both domain scenarios and the DR drill run on the k3d cluster from manifests generated out
  of the compose files; six stacks × nine phases verified, backups/restores/DR drills all ✅ (`results/k3s/`).
- The NodePort hop costs ~0.3 ms on point operations (Redis GET 0.21 → 0.50 ms); bulk numbers are within noise.
- Failover measured with SIGKILL semantics differs in kind from compose: a deleted pod's network namespace disappears,
  so MongoDB elects in 0.9 s (30 s under `docker kill`), Neo4j Raft in 11 s (21 s), ES in 1.0 s (4.9 s).
- Traps: Kubernetes service-link env vars (`NEO4J_NP_PORT_7687_TCP`) crash Neo4j's strict config parser →
  `enableServiceLinks: false`; ES `bootstrap.memory_lock` fails the bootstrap check without a memlock ulimit; Cassandra
  needs seeds by Service name + a NodePort per node + JMX opened for remote `nodetool`; Neo4j routing must advertise
  the host-reachable nodePorts; `kubectl cp` needs `tar` in the image (Elasticsearch has none → busybox helper pod);
  a Deployment scaled to 0 has no pod, so lifecycle actions resolve the Deployment by label; two k3d clusters on one host
  need disjoint nodePort→loopback ranges; the shared Harbor can be stopped by the other lab's session mid-run (images
  already pulled keep working, new pulls do not).

## Portability traps found
- MongoDB 8 will not start on this host's Linux 7.0 kernel (SERVER-121912); the lab pins `mongo:7`.
- Cassandra: bind `UNSET_VALUE`, never `NULL`, or you write tombstones that later break `ALLOW FILTERING` scans.
- Elasticsearch: offset pagination past `index.max_result_window` (10 000) is a hard 400; `refresh=wait_for` blocks a
  write until the next refresh cycle.
- Embedded KV stores: one process may open the file once — share a single handle across threads.
