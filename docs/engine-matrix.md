# NoSQL engine research matrix (Sept 2026)

Status: **R** = pulled/run and measured in this lab, **A** = adapter written, stack not yet added, **X** = evaluated, excluded.

| engine | family | image:tag | version seen | protocol / API | python driver | replication in lab | status |
|---|---|---|---|---|---|---|---|
| Redis | key-value | `redis:8` | 8.10.1 (Query Engine + JSON + vector modules built in) | RESP3 | redis-py | primary→replica async + 3 Sentinels + HAProxy read LB | R |
| MongoDB | document | `mongo:7` | 7.0.37 | MongoDB wire | pymongo | 3-member replica set (oplog, auto-election) | R |
| Cassandra | wide-column | `cassandra:5` | 5.0.9 (CQL 3.4.7) | CQL native | cassandra-driver (libev) | leaderless RF=3, LOCAL_QUORUM | R |
| Neo4j | graph | `neo4j:5-community` | 5.26.30 Community | Bolt | neo4j | none (Community cannot replicate) | R |
| Neo4j Enterprise cluster | graph | `neo4j:5-enterprise` (evaluation licence) | 5.26.30 Enterprise | Bolt (`neo4j://` routing) | neo4j | 3 primaries, Raft leader election; online backup, seedURI restore | R |
| Elasticsearch | search | `elasticsearch:9.5.3` | 9.5.3 | HTTP/REST | httpx (raw REST) | 3 nodes, 1 shard + 2 replicas per index, master election | R |
| LMDB | embedded | python `lmdb` | LMDB 0.9.35 / python-lmdb 2.3.0 | in-process | lmdb | none | R |
| RocksDB | embedded | python `rocksdict` | RocksDB via rocksdict 0.3.29 | in-process | rocksdict | none | R |
| Valkey / Dragonfly / KeyDB / Garnet | key-value | `valkey/valkey`, `dragonflydb/dragonfly`, ... | — | RESP | redis-py | same RESP adapter (`features.cluster`, `features.search`) | A |
| ScyllaDB | wide-column | `scylladb/scylla` | — | CQL (v4 protocol) | cassandra-driver | same CQL adapter (`features.flavor: scylla`) | A |
| YugabyteDB YCQL | wide-column | `yugabytedb/yugabyte` | — | CQL | cassandra-driver | same CQL adapter (`features.flavor: yugabyte`) | A |
| FerretDB / DocumentDB | document | `ghcr.io/ferretdb/ferretdb`, `microsoft/documentdb` | — | MongoDB wire (on Postgres) | pymongo | same pymongo adapter | A |
| Memgraph | graph | `memgraph/memgraph` | — | Bolt | neo4j | same Bolt adapter | A |
| OpenSearch | search | `opensearchproject/opensearch` | — | HTTP/REST | httpx | same ES adapter (`features.distribution: opensearch`) | A |
| Couchbase | document | `couchbase` | — | Couchbase SDK / N1QL | couchbase | driver registered (`couchbase`), adapter module TODO | A |
| Aerospike | key-value | `aerospike/aerospike-server` | — | Aerospike wire | aerospike | driver registered, adapter TODO | A |
| Qdrant / Weaviate / Milvus / Chroma | vector | `qdrant/qdrant`, `semitechnologies/weaviate`, `milvusdb/milvus`, `chromadb/chroma` | — | HTTP / gRPC | qdrant-client / weaviate-client / pymilvus / chromadb | drivers registered, adapters TODO | A |
| SurrealDB | multi-model | `surrealdb/surrealdb` | — | HTTP / WS | surrealdb | driver registered, adapter TODO | A |
| ArangoDB | multi-model | `arangodb` | — | HTTP | python-arango | driver registered, adapter TODO | A |
| etcd | key-value (CP) | `quay.io/coreos/etcd` | — | gRPC | etcd3 | driver registered, adapter TODO | A |
| Memcached | cache | `memcached` | — | memcached text/meta | pymemcache | driver registered, adapter TODO | A |
| Tarantool | in-memory app-server | `tarantool/tarantool` | — | iproto | tarantool | driver registered, adapter TODO | A |
| RavenDB | document | `ravendb/ravendb` | — | HTTP | (pyravendb) | driver registered, adapter TODO | A |
| FalkorDB | graph (on Redis) | `falkordb/falkordb` | — | RESP + GRAPH.QUERY | falkordb | driver registered, adapter TODO | A |
| InfluxDB / QuestDB-ILP | time-series | `influxdb`, `questdb/questdb` | — | HTTP / line protocol | httpx | adapter TODO (QuestDB SQL side lives in rd-databases) | A |
| MongoDB 8.x | document | `mongo:8` | 8.3.9 | MongoDB wire | pymongo | **X on this host**: crashes at startup on Linux 6.19+/7.0 kernels (SERVER-121912, TCMalloc rseq); fixed in 8.0.30+/8.3.9+... use `mongo:7` here |
| Cassandra 5.1 analyzed text SAI | wide-column | — | 5.1 | CQL | — | X: 5.0.9 SAI has no `index_analyzer` option; analyzed full-text search needs 5.1 |

## Notes learned while building

- **MongoDB 8 vs the host kernel.** `mongo:8` (8.3.9) refuses to start on this box's Linux 7.0.0 kernel (`MongoDB cannot start: Linux kernel versions 6.19 and newer has a known incompatibility`, SERVER-121912 — a TCMalloc `rseq` ABI break). The `GLIBC_TUNABLES=glibc.pthread.rseq=0` workaround did **not** help. Fix is MongoDB ≥ 8.0.30 / 8.3.9-with-the-patch or a kernel < 6.19; the lab pins `mongo:7` (7.0.37), which is unaffected.
- **cassandra-driver** needs the compiled **libev** reactor for throughput (`cassandra.io.libevreactor.LibevConnection`), and every `INSERT` must bind `UNSET_VALUE` (not `None`) for absent columns — a bound `NULL` writes a **tombstone**, and 100k+ tombstones make `ALLOW FILTERING` scans fail with `ReadFailure`. SAI **vector** ingestion (jvector) throws `Number of outstanding pooled objects has gone beyond the limit` above ~32 in-flight writes, so the products table loads at lower concurrency.
- **Cassandra SAI on a low-cardinality column + range** (`event_type` has 7 values, then a timestamp range over ~80k matching rows) hits `ReadFailure`; a **denormalised query table** `((event_type, month), occurred_at, id)` answers the same query in ~6 ms. This is the query-first lesson, measured.
- **Embedded stores** (LMDB, RocksDB) refuse a second open of the same file in one process, so the harness keeps **one process-global handle** and shares it across threads. Python threads do not scale reads (GIL): LMDB does ~99k point reads/s single-threaded but only ~30k with 16 threads.
- **Elasticsearch** offset pagination beyond `index.max_result_window` (default 10 000) returns HTTP 400 by design — the lab raises the window on `orders` so `from/size` can be compared against `search_after`. `refresh=wait_for` on a write blocks until the next refresh cycle (30 s here); `refresh=true` forces it (~27 ms).
- **Redis** models documents as hashes with application-maintained index sets/zsets; the RediSearch module (built into Redis 8) provides real text + HNSW vector indexes. `WAIT n timeout` gives a per-write synchronous replica ack (~10–30 ms here).

## Registries used
Docker Hub images via `mirror.gcr.io` (see `scripts/pull-images.sh`); `docker.elastic.co` for Elasticsearch; `mcr.microsoft.com`, `icr.io`, `ghcr.io`, `quay.io` pulled directly.

## Backup & logging facts per engine (measured 2026-09-14, details in `backup-logging.md`)
| engine | backup tool (online?) | restore | incremental / PITR | slow-query log | audit log | JSON log | run-time log level |
|---|---|---|---|---|---|---|---|
| Redis 8 | `BGSAVE` + copy RDB (online) | offline: RDB as multi-part AOF base | no / no (AOF = crash safety) | `SLOWLOG` + `LATENCY` | `ACL LOG` (denials) | no | `CONFIG SET loglevel` |
| MongoDB 7 | `mongodump --oplog` (online) | online `mongorestore --oplogReplay` | Community no / oplog (PBM) | profiler + `system.profile` + JSON "Slow query" | Enterprise/PSMDB | yes | `logComponentVerbosity` |
| Cassandra 5.0 | `nodetool snapshot --ttl` + export (online) | online `nodetool import --copy-data` per node (sstableloader breaks on `vector<>`) | `enablebackup` / commitlog archiving | `slow_query_log_timeout` 500 ms (yaml) → debug.log | `enableauditlog` + `auditlogviewer`, FQL | no (logback text) | `nodetool setlogginglevel` |
| Neo4j 5 Community | `neo4j-admin database dump` (**offline**) | offline `database load` | Enterprise / Enterprise | query.log empty in CE → `db.stats` query collector | Enterprise | via log4j2 layout | log4j2 `monitorInterval` reload (30 s) |
| Neo4j 5 Enterprise cluster | `neo4j-admin database backup --from=<member>:6362` (online) | `DROP DATABASE` + `CREATE DATABASE ... seedURI:'file://...'` (every primary seeds; database offline meanwhile) | differential backups / transaction-log PITR | `db.logs.query.*` → query.log (works) | security.log | via log4j2 layout | `dbms.setConfigValue` (dynamic settings) |
| Elasticsearch 9 (3 nodes) | `_snapshot` repository on a volume shared by every node (online) | online `_restore` | segment-level yes / no | per-index slowlog thresholds (written by the data node that ran the shard query) | needs security | ECS JSON | `logger.*` cluster setting |
| LMDB | `env.copy(compact)` (online) | replace file, reopen | no / no | app | app | — | — |
| RocksDB | `Checkpoint` (online) | replace dir, reopen | BackupEngine (C++) / no | app | app | info LOG (text) | open-time options |
