# Engines

One deterministic e-commerce dataset (the same values as the relational lab `rd-databases`) loaded into every
engine, then a shared operation catalog timed against each. This table is the fleet overview: what was measured,
which image/version, how fast the initial load went, and the replication topology each engine ran with.

Generated: 2026-09-14T15:45:15+00:00.

| engine | family | version | load events/s | load s | replication |
|---|---|---|---:|---:|---|
| MongoDB 7.0 | document — document store (replica set) | MongoDB 7.0.37 | 46,677 | 35.67 | replica set (oplog, async, automatic election) |
| MongoDB 7.0 (k3s) | document — document store (replica set) | MongoDB 7.0.37 | 46,916 | 35.62 | replica set (oplog, async, automatic election) |
| LMDB | embedded — embedded ordered key-value (memory-mapped B+tree) | LMDB 0.9.35 (python-lmdb 2.3.0) | 25,872 | 58.34 | none |
| RocksDB | embedded — embedded ordered key-value (LSM tree) | RocksDB via rocksdict ? | 26,625 | 59.32 | none |
| Neo4j 5 | graph — property graph (native, index-free adjacency) | Neo4j Kernel 5.26.30 (community) | 9,248 | 174.37 | none (single instance; Enterprise causal clustering not in Community) |
| Neo4j 5 (k3s) | graph — property graph (native, index-free adjacency) | Neo4j Kernel 5.26.30 (community) | 7,281 | 201.32 | none (single instance; Enterprise causal clustering not in Community) |
| Neo4j 5 Enterprise cluster | graph — property graph (native, index-free adjacency) — 3-primary Raft cluster | Neo4j Kernel 5.26.30 (enterprise) | 7,314 | 209.1 | Raft cluster of 3 primaries (leader takes writes, followers replicate; automatic leader election) |
| Neo4j 5 Enterprise cluster (k3s) | graph — property graph (native, index-free adjacency) — 3-primary Raft cluster | Neo4j Kernel 5.26.30 (enterprise) | 8,008 | 197.49 | Raft cluster of 3 primaries (leader takes writes, followers replicate; automatic leader election) |
| Redis 8 | keyvalue — in-memory key-value (data structures, Query Engine, JSON) | redis 8.10.1 | 41,922 | 42.97 | async primary/replica (diskless sync, AOF everysec) + Sentinel failover |
| Redis 8 (k3s) | keyvalue — in-memory key-value (data structures, Query Engine, JSON) | redis 8.10.1 | 41,993 | 42.46 | async primary/replica (diskless sync, AOF everysec) + Sentinel failover |
| Elasticsearch 9 | search — search / analytics engine (Lucene, inverted index) | elasticsearch 9.5.3 | 29,495 | 68.53 | primary + replica shards (number_of_replicas 2) over 3 nodes; master election by quorum |
| Elasticsearch 9 (k3s) | search — search / analytics engine (Lucene, inverted index) | elasticsearch 9.5.3 | 33,762 | 63.82 | primary + replica shards (number_of_replicas 2) over 3 nodes; master election by quorum |
| Cassandra 5.0 | widecolumn — wide-column (leaderless, tunable consistency) | Cassandra 5.0.9 (CQL 3.4.7) | 17,454 | 167.23 | leaderless replication, RF=3, tunable consistency (LOCAL_QUORUM) |
| Cassandra 5.0 (k3s) | widecolumn — wide-column (leaderless, tunable consistency) | Cassandra 5.0.9 (CQL 3.4.7) | 17,806 | 165.55 | leaderless replication, RF=3, tunable consistency (LOCAL_QUORUM) |

## Load detail (per collection)

| engine | collection | rps | seconds | method |
|---|---|---:|---:|---|
| MongoDB 7.0 | customers | 17,633 | 1.13 | insert_many(5000, ordered=False, w=1) |
| MongoDB 7.0 | categories | 50,348 | 0.0 | insert_many(5000, ordered=False, w=1) |
| MongoDB 7.0 | products | 6,191 | 0.81 | insert_many(5000, ordered=False, w=1) |
| MongoDB 7.0 | orders | 37,090 | 5.39 | insert_many(5000, ordered=False, w=1) x4 connections |
| MongoDB 7.0 | order_items | 92,255 | 5.29 | insert_many(5000, ordered=False, w=1) x4 connections |
| MongoDB 7.0 | events | 46,677 | 21.42 | insert_many(5000, ordered=False, w=1) x4 connections |
| MongoDB 7.0 | inventory | 35,589 | 0.56 | insert_many(5000, ordered=False, w=1) |
| MongoDB 7.0 (k3s) | customers | 17,973 | 1.11 | insert_many(5000, ordered=False, w=1) |
| MongoDB 7.0 (k3s) | categories | 35,550 | 0.01 | insert_many(5000, ordered=False, w=1) |
| MongoDB 7.0 (k3s) | products | 6,129 | 0.82 | insert_many(5000, ordered=False, w=1) |
| MongoDB 7.0 (k3s) | orders | 36,551 | 5.47 | insert_many(5000, ordered=False, w=1) x4 connections |
| MongoDB 7.0 (k3s) | order_items | 92,250 | 5.29 | insert_many(5000, ordered=False, w=1) x4 connections |
| MongoDB 7.0 (k3s) | events | 46,916 | 21.31 | insert_many(5000, ordered=False, w=1) x4 connections |
| MongoDB 7.0 (k3s) | inventory | 33,710 | 0.59 | insert_many(5000, ordered=False, w=1) |
| LMDB | customers | 66,058 | 0.3 | bulk_put |
| LMDB | categories | 114,456 | 0.0 | bulk_put |
| LMDB | products | 30,232 | 0.17 | bulk_put |
| LMDB | orders | 17,003 | 11.76 | bulk_put |
| LMDB | order_items | 82,893 | 5.88 | bulk_put |
| LMDB | events | 25,872 | 38.65 | bulk_put |
| LMDB | inventory | 110,195 | 0.18 | bulk_put |
| RocksDB | customers | 62,168 | 0.32 | bulk_put |
| RocksDB | categories | 104,401 | 0.0 | bulk_put |
| RocksDB | products | 26,643 | 0.19 | bulk_put |
| RocksDB | orders | 19,305 | 10.36 | bulk_put |
| RocksDB | order_items | 74,794 | 6.52 | bulk_put |
| RocksDB | events | 26,625 | 37.56 | bulk_put |
| RocksDB | inventory | 111,276 | 0.18 | bulk_put |
| Neo4j 5 | customers | 4,456 | 4.49 | bulk_put |
| Neo4j 5 | categories | 393 | 0.51 | bulk_put |
| Neo4j 5 | products | 1,191 | 4.2 | bulk_put |
| Neo4j 5 | orders | 3,616 | 55.31 | bulk_put |
| Neo4j 5 | events | 9,248 | 108.13 | bulk_put |
| Neo4j 5 | inventory | 13,050 | 1.53 | bulk_put |
| Neo4j 5 (k3s) | customers | 4,642 | 4.31 | bulk_put |
| Neo4j 5 (k3s) | categories | 538 | 0.37 | bulk_put |
| Neo4j 5 (k3s) | products | 2,023 | 2.47 | bulk_put |
| Neo4j 5 (k3s) | orders | 3,634 | 55.04 | bulk_put |
| Neo4j 5 (k3s) | events | 7,281 | 137.34 | bulk_put |
| Neo4j 5 (k3s) | inventory | 12,428 | 1.61 | bulk_put |
| Neo4j 5 Enterprise cluster | customers | 3,904 | 5.12 | bulk_put |
| Neo4j 5 Enterprise cluster | categories | 668 | 0.3 | bulk_put |
| Neo4j 5 Enterprise cluster | products | 1,583 | 3.16 | bulk_put |
| Neo4j 5 Enterprise cluster | orders | 3,262 | 61.32 | bulk_put |
| Neo4j 5 Enterprise cluster | events | 7,314 | 136.73 | bulk_put |
| Neo4j 5 Enterprise cluster | inventory | 8,934 | 2.24 | bulk_put |
| Neo4j 5 Enterprise cluster (k3s) | customers | 4,197 | 4.77 | bulk_put |
| Neo4j 5 Enterprise cluster (k3s) | categories | 665 | 0.3 | bulk_put |
| Neo4j 5 Enterprise cluster (k3s) | products | 1,487 | 3.36 | bulk_put |
| Neo4j 5 Enterprise cluster (k3s) | orders | 3,227 | 61.98 | bulk_put |
| Neo4j 5 Enterprise cluster (k3s) | events | 8,008 | 124.88 | bulk_put |
| Neo4j 5 Enterprise cluster (k3s) | inventory | 10,449 | 1.91 | bulk_put |
| Redis 8 | customers | 13,352 | 1.5 | pipeline HSET (2000/batch) + index sets |
| Redis 8 | categories | 25,396 | 0.01 | pipeline HSET (2000/batch) + index sets |
| Redis 8 | products | 5,481 | 0.91 | pipeline HSET (2000/batch) + index sets |
| Redis 8 | orders | 24,506 | 8.16 | pipeline HSET (2000/batch) + index sets x4 connections |
| Redis 8 | order_items | 62,656 | 7.78 | pipeline HSET (2000/batch) + index sets x4 connections |
| Redis 8 | events | 41,922 | 23.85 | pipeline HSET (2000/batch) + index sets x4 connections |
| Redis 8 | inventory | 27,083 | 0.74 | pipeline HSET (2000/batch) + index sets |
| Redis 8 (k3s) | customers | 13,143 | 1.52 | pipeline HSET (2000/batch) + index sets |
| Redis 8 (k3s) | categories | 28,419 | 0.01 | pipeline HSET (2000/batch) + index sets |
| Redis 8 (k3s) | products | 5,732 | 0.87 | pipeline HSET (2000/batch) + index sets |
| Redis 8 (k3s) | orders | 28,315 | 7.06 | pipeline HSET (2000/batch) + index sets x4 connections |
| Redis 8 (k3s) | order_items | 57,452 | 8.49 | pipeline HSET (2000/batch) + index sets x4 connections |
| Redis 8 (k3s) | events | 41,993 | 23.81 | pipeline HSET (2000/batch) + index sets x4 connections |
| Redis 8 (k3s) | inventory | 29,417 | 0.68 | pipeline HSET (2000/batch) + index sets |
| Elasticsearch 9 | customers | 4,549 | 4.4 | bulk_put |
| Elasticsearch 9 | categories | 6,753 | 0.03 | bulk_put |
| Elasticsearch 9 | products | 1,760 | 2.84 | bulk_put |
| Elasticsearch 9 | orders | 23,793 | 8.41 | bulk_put x4 connections |
| Elasticsearch 9 | order_items | 40,104 | 12.16 | bulk_put x4 connections |
| Elasticsearch 9 | events | 29,495 | 33.9 | bulk_put x4 connections |
| Elasticsearch 9 | inventory | 9,947 | 2.01 | bulk_put |
| Elasticsearch 9 (k3s) | customers | 4,673 | 4.28 | bulk_put |
| Elasticsearch 9 (k3s) | categories | 6,244 | 0.03 | bulk_put |
| Elasticsearch 9 (k3s) | products | 1,697 | 2.95 | bulk_put |
| Elasticsearch 9 (k3s) | orders | 24,452 | 8.18 | bulk_put x4 connections |
| Elasticsearch 9 (k3s) | order_items | 61,502 | 7.93 | bulk_put x4 connections |
| Elasticsearch 9 (k3s) | events | 33,762 | 29.62 | bulk_put x4 connections |
| Elasticsearch 9 (k3s) | inventory | 12,298 | 1.63 | bulk_put |
| Cassandra 5.0 | customers | 3,425 | 5.84 | execute_concurrent (prepared, 256 in flight, CL=ONE) |
| Cassandra 5.0 | categories | 3,296 | 0.06 | execute_concurrent (prepared, 256 in flight, CL=ONE) |
| Cassandra 5.0 | products | 1,358 | 3.68 | execute_concurrent (prepared, 256 in flight, CL=ONE) |
| Cassandra 5.0 | orders | 7,331 | 27.28 | execute_concurrent (prepared, 256 in flight, CL=ONE) x8 connections |
| Cassandra 5.0 | order_items | 13,747 | 35.47 | execute_concurrent (prepared, 256 in flight, CL=ONE) x8 connections |
| Cassandra 5.0 | events | 17,454 | 57.29 | execute_concurrent (prepared, 256 in flight, CL=ONE) x8 connections |
| Cassandra 5.0 | inventory | 2,063 | 9.7 | execute_concurrent (prepared, 256 in flight, CL=ONE) |
| Cassandra 5.0 (k3s) | customers | 3,230 | 6.19 | execute_concurrent (prepared, 256 in flight, CL=ONE) |
| Cassandra 5.0 (k3s) | categories | 2,882 | 0.07 | execute_concurrent (prepared, 256 in flight, CL=ONE) |
| Cassandra 5.0 (k3s) | products | 1,440 | 3.47 | execute_concurrent (prepared, 256 in flight, CL=ONE) |
| Cassandra 5.0 (k3s) | orders | 7,464 | 26.8 | execute_concurrent (prepared, 256 in flight, CL=ONE) x8 connections |
| Cassandra 5.0 (k3s) | order_items | 13,918 | 35.03 | execute_concurrent (prepared, 256 in flight, CL=ONE) x8 connections |
| Cassandra 5.0 (k3s) | events | 17,806 | 56.16 | execute_concurrent (prepared, 256 in flight, CL=ONE) x8 connections |
| Cassandra 5.0 (k3s) | inventory | 2,062 | 9.7 | execute_concurrent (prepared, 256 in flight, CL=ONE) |

## Engine notes

- **MongoDB 7.0** (`mongo:7`): Official mongo:7 image (7.0.x). mongo:8 (8.3.9) refuses to start on this host's Linux 7.0.0 kernel (SERVER-121912: TCMalloc rseq incompatibility with kernels 6.19-7.0.13, exits at startup, no bypass) so the lab pins 7.0. 3-member replica set rs0 (priorities 3/2/1) initiated by a one-shot mongosh container; no auth. Per-node targets use directConnection=true; writes w=1 unless an experiment says otherwise. $vectorSearch is Atlas / mongot-only, so vector_knn is unsupported on Community.
- **MongoDB 7.0 (k3s)** (`mongo:7`): Official mongo:7 image (7.0.x). mongo:8 (8.3.9) refuses to start on this host's Linux 7.0.0 kernel (SERVER-121912: TCMalloc rseq incompatibility with kernels 6.19-7.0.13, exits at startup, no bypass) so the lab pins 7.0. 3-member replica set rs0 (priorities 3/2/1) initiated by a one-shot mongosh container; no auth. Per-node targets use directConnection=true; writes w=1 unless an experiment says otherwise. $vectorSearch is Atlas / mongot-only, so vector_knn is unsupported on Community.
- **LMDB** (`(embedded, python lmdb package)`): Lightning Memory-Mapped Database, in-process, single writer / many MVCC readers. No query engine: an ordered key-value map. The lab models documents plus hand-maintained ordered index keyspaces (idx/...); aggregations, text, geo and vector scan the primary keyspace. 8 GB map. No server, no network, no replication.
- **RocksDB** (`(embedded, python rocksdict/RocksDB)`): RocksDB (Facebook LSM-tree) via rocksdict, in-process, thread-safe. Same modelling as LMDB (ordered key-value + hand-maintained index keyspaces, WriteBatch for atomic multi-key writes). Write-optimised (LSM) vs LMDB's read-optimised B+tree. No server, no network, no replication.
- **Neo4j 5** (`neo4j:5-community`): Neo4j 5 Community, single instance (causal clustering, CDC, online backup and read routing are Enterprise-only). Order items are CONTAINS relationships, categories a CHILD_OF tree, so graph_traversal and join_lookup are native. Built-in full-text (Lucene) and HNSW vector indexes; no native TTL or pub/sub.
- **Neo4j 5 (k3s)** (`neo4j:5-community`): Neo4j 5 Community, single instance (causal clustering, CDC, online backup and read routing are Enterprise-only). Order items are CONTAINS relationships, categories a CHILD_OF tree, so graph_traversal and join_lookup are native. Built-in full-text (Lucene) and HNSW vector indexes; no native TTL or pub/sub.
- **Neo4j 5 Enterprise cluster** (`neo4j:5-enterprise`): Neo4j 5 Enterprise (evaluation licence) with 3 primaries in one Raft cluster: the elected leader takes writes, followers replicate and reject direct writes, a new leader is elected when the leader dies. The `primary` target is a routing URI (neo4j://) so writes always reach the leader; node2/node3 are direct Bolt connections to individual members. The dataset lives in a database called `lab` (created by the harness) so it can be dropped and re-seeded from an online backup (neo4j-admin database backup + CREATE DATABASE ... seedURI). Same graph model and indexes as the Community stack. Licence: Enterprise features are used under Neo4j's evaluation terms only.
- **Neo4j 5 Enterprise cluster (k3s)** (`neo4j:5-enterprise`): Neo4j 5 Enterprise (evaluation licence) with 3 primaries in one Raft cluster: the elected leader takes writes, followers replicate and reject direct writes, a new leader is elected when the leader dies. The `primary` target is a routing URI (neo4j://) so writes always reach the leader; node2/node3 are direct Bolt connections to individual members. The dataset lives in a database called `lab` (created by the harness) so it can be dropped and re-seeded from an online backup (neo4j-admin database backup + CREATE DATABASE ... seedURI). Same graph model and indexes as the Community stack. Licence: Enterprise features are used under Neo4j's evaluation terms only.
- **Redis 8** (`redis:8`): Redis Open Source 8 (RediSearch/JSON/TimeSeries/probabilistic modules built in). Async primary->replica replication with diskless sync, AOF everysec on both; 3 Sentinels (quorum 2, down-after 2 s) elect a new primary; HAProxy round-robins reads over primary+replica. Documents are hashes with application-maintained index sets/zsets; FT.CREATE on products for text + vector.
- **Redis 8 (k3s)** (`redis:8`): Redis Open Source 8 (RediSearch/JSON/TimeSeries/probabilistic modules built in). Async primary->replica replication with diskless sync, AOF everysec on both; 3 Sentinels (quorum 2, down-after 2 s) elect a new primary; HAProxy round-robins reads over primary+replica. Documents are hashes with application-maintained index sets/zsets; FT.CREATE on products for text + vector.
- **Elasticsearch 9** (`elasticsearch:9.5.3`): Elasticsearch 9, 3-node cluster (all master-eligible + data), security disabled. One index per collection with 1 primary shard + 2 replicas, so every node holds every shard (the RF=3 equivalent of the other clustered stacks). Every field indexed at mapping time (inverted index for keyword/text, BKD tree for numeric/geo, HNSW for dense_vector). Eventually consistent for search (30 s refresh; the harness refreshes after load). No multi-document transactions, no server-side joins across indices, no per-document TTL. Any node coordinates writes; the master is re-elected by the remaining quorum when a node dies.
- **Elasticsearch 9 (k3s)** (`elasticsearch:9.5.3`): Elasticsearch 9, 3-node cluster (all master-eligible + data), security disabled. One index per collection with 1 primary shard + 2 replicas, so every node holds every shard (the RF=3 equivalent of the other clustered stacks). Every field indexed at mapping time (inverted index for keyword/text, BKD tree for numeric/geo, HNSW for dense_vector). Eventually consistent for search (30 s refresh; the harness refreshes after load). No multi-document transactions, no server-side joins across indices, no per-document TTL. Any node coordinates writes; the master is re-elected by the remaining quorum when a node dies.
- **Cassandra 5.0** (`cassandra:5`): Official cassandra:5 image, 3 nodes RF=3, LOCAL_QUORUM by default. Query-first schema (one table per access path), SAI indexes for vector (ANN) and analyzed text search, LWT for compare-and-set, counter table for increments. Nodes are addressed by static container IPs (10.99.1.11-13); the client is pinned to one node per target.
- **Cassandra 5.0 (k3s)** (`cassandra:5`): Official cassandra:5 image, 3 nodes RF=3, LOCAL_QUORUM by default. Query-first schema (one table per access path), SAI indexes for vector (ANN) and analyzed text search, LWT for compare-and-set, counter table for increments. Nodes are addressed by static container IPs (10.99.1.11-13); the client is pinned to one node per target.
