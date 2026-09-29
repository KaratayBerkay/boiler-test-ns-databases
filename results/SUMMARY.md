# ns-databases — cross-engine results

Generated 2026-09-14T15:45:15+00:00 from `results/<engine>/latest.json`. Timings are client-observed p50 latencies in milliseconds (warm cache, single client, 10 iterations unless noted). `--` = the engine has no way to express the operation, `ERR` = failed at run time.

## Engines

| engine | version | family | category | image | load docs/s (events) | schema+load s | replication |
|---|---|---|---|---|---:|---:|---|
| Cassandra 5.0 | Cassandra 5.0.9 (CQL 3.4.7) | widecolumn | wide-column (leaderless, tunable consistency) | `cassandra:5` | 17454 | 167.23 | leaderless replication, RF=3, tunable consistency (LOCAL_QUORUM) |
| Elasticsearch 9 | elasticsearch 9.5.3 | search | search / analytics engine (Lucene, inverted index) | `elasticsearch:9.5.3` | 29495 | 68.53 | primary + replica shards (number_of_replicas 2) over 3 nodes; master election by quorum |
| LMDB | LMDB 0.9.35 (python-lmdb 2.3.0) | embedded | embedded ordered key-value (memory-mapped B+tree) | `(embedded, python lmdb package)` | 25872 | 58.34 | none |
| MongoDB 7.0 | MongoDB 7.0.37 | document | document store (replica set) | `mongo:7` | 46677 | 35.67 | replica set (oplog, async, automatic election) |
| Neo4j 5 | Neo4j Kernel 5.26.30 (community) | graph | property graph (native, index-free adjacency) | `neo4j:5-community` | 9248 | 174.37 | none (single instance; Enterprise causal clustering not in Community) |
| Neo4j 5 Enterprise cluster | Neo4j Kernel 5.26.30 (enterprise) | graph | property graph (native, index-free adjacency) — 3-primary Raft cluster | `neo4j:5-enterprise` | 7314 | 209.1 | Raft cluster of 3 primaries (leader takes writes, followers replicate; automatic leader election) |
| Redis 8 | redis 8.10.1 | keyvalue | in-memory key-value (data structures, Query Engine, JSON) | `redis:8` | 41922 | 42.97 | async primary/replica (diskless sync, AOF everysec) + Sentinel failover |
| RocksDB | RocksDB via rocksdict ? | embedded | embedded ordered key-value (LSM tree) | `(embedded, python rocksdict/RocksDB)` | 26625 | 59.32 | none |
| Cassandra 5.0 (k3s) | Cassandra 5.0.9 (CQL 3.4.7) | widecolumn | wide-column (leaderless, tunable consistency) | `cassandra:5` | 17806 | 165.55 | leaderless replication, RF=3, tunable consistency (LOCAL_QUORUM) |
| Elasticsearch 9 (k3s) | elasticsearch 9.5.3 | search | search / analytics engine (Lucene, inverted index) | `elasticsearch:9.5.3` | 33762 | 63.82 | primary + replica shards (number_of_replicas 2) over 3 nodes; master election by quorum |
| MongoDB 7.0 (k3s) | MongoDB 7.0.37 | document | document store (replica set) | `mongo:7` | 46916 | 35.62 | replica set (oplog, async, automatic election) |
| Neo4j 5 (k3s) | Neo4j Kernel 5.26.30 (community) | graph | property graph (native, index-free adjacency) | `neo4j:5-community` | 7281 | 201.32 | none (single instance; Enterprise causal clustering not in Community) |
| Neo4j 5 Enterprise cluster (k3s) | Neo4j Kernel 5.26.30 (enterprise) | graph | property graph (native, index-free adjacency) — 3-primary Raft cluster | `neo4j:5-enterprise` | 8008 | 197.49 | Raft cluster of 3 primaries (leader takes writes, followers replicate; automatic leader election) |
| Redis 8 (k3s) | redis 8.10.1 | keyvalue | in-memory key-value (data structures, Query Engine, JSON) | `redis:8` | 41993 | 42.46 | async primary/replica (diskless sync, AOF everysec) + Sentinel failover |

## Operation capability & latency matrix (p50 ms)

| operation | Cassandra 5.0 | Elasticsearch 9 | LMDB | MongoDB 7.0 | Neo4j 5 | Neo4j 5 Enterprise cluster | Redis 8 | RocksDB | Cassandra 5.0 (k3s) | Elasticsearch 9 (k3s) | MongoDB 7.0 (k3s) | Neo4j 5 (k3s) | Neo4j 5 Enterprise cluster (k3s) | Redis 8 (k3s) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `ping` | 1.15 | 2.24 | 0.00 | 0.29 | 2.09 | 3.83 | 0.20 | 0.00 | 1.72 | 2.60 | 0.58 | 2.78 | 2.65 | 0.47 |
| `get_by_key` | 1.77 | 3.92 | 0.02 | 0.79 | 2.65 | 2.63 | 0.21 | 0.03 | 2.58 | 4.33 | 1.08 | 2.82 | 3.62 | 0.50 |
| `multi_get` | 22 | 25 | 0.84 | 2.16 | 14 | 8.65 | 1.90 | 1.85 | 23 | 29 | 2.66 | 14 | 12 | 3.62 |
| `insert_one` | 1.25 | 7.07 | 0.07 | 3.96 | 5.92 | 10 | 0.30 | 0.08 | 1.14 | 7.42 | 4.00 | 5.28 | 8.28 | 0.97 |
| `update_field` | 0.87 | 9.12 | 0.05 | 4.53 | 4.46 | 9.19 | 0.23 | 0.05 | 1.48 | 8.85 | 4.57 | 5.45 | 7.22 | 0.61 |
| `upsert` | 0.87 | 7.22 | 0.05 | 3.61 | 4.70 | 9.07 | 0.30 | 0.05 | 1.72 | 7.88 | 4.54 | 5.54 | 6.78 | 0.79 |
| `delete_one` | 0.81 | 5.38 | 0.03 | 4.46 | 5.51 | 8.04 | 0.26 | 0.01 | 1.33 | 6.56 | 2.63 | 5.61 | 8.69 | 0.66 |
| `atomic_increment` | 3.38 | 11 | 0.04 | 4.92 | 3.79 | 8.66 | 0.24 | 0.03 | 4.87 | 11 | 3.81 | 5.37 | 7.55 | 0.49 |
| `conditional_update` | 5.85 | 16 | 0.05 | 6.07 | 5.15 | 9.87 | 0.53 | 0.05 | 7.67 | 15 | 4.15 | 6.03 | 7.22 | 1.29 |
| `batch_insert_1000` | 246 | 110 | 23 | 32 | 131 | 166 | 45 | 24 | 235 | 105 | 26 | 124 | 195 | 50 |
| `range_by_secondary` | 2.21 | 7.52 | 0.06 | 0.77 | 2.83 | 3.65 | 0.70 | 0.07 | 1.55 | 7.40 | 0.80 | 3.27 | 3.49 | 0.64 |
| `range_by_time` | 4.76 s | 27 | 739 | 599 | 74 | 57 | 2.61 | 880 | 4.35 s | 25 | 668 | 77 | 59 | 2.78 |
| `filter_nested` | 93 | 4.78 | 130 | 1.30 | 16 | 14 | 16 | 137 | 89 | 4.83 | 0.91 | 15 | 23 | 17 |
| `array_contains` | 78 | 4.90 | 45 | 2.94 | 7.51 | 13 | 11 | 46 | 84 | 5.06 | 2.50 | 8.17 | 15 | 12 |
| `top_n_sorted` | 2.18 | 9.59 | 0.06 | 1.28 | 3.08 | 3.02 | 0.97 | 0.08 | 2.75 | 8.78 | 0.85 | 3.57 | 3.75 | 0.92 |
| `pagination_offset` | 77 | 61 | 7.97 | 8.49 | 261 | 201 | 1.43 | 7.86 | 93 | 61 | 7.96 | 303 | 245 | 1.28 |
| `pagination_keyset` | 2.73 | 9.91 | 0.58 | 2.02 | 236 | 195 | 1.55 | 1.13 | 3.46 | 15 | 1.31 | 242 | 257 | 1.57 |
| `count_filtered` | 793 | 4.46 | 11 | 9.10 | 127 | 73 | 0.22 | 11 | 1.08 s | 4.51 | 9.16 | 117 | 91 | 0.25 |
| `aggregate_group` | 4.88 s | 6.57 | 1.68 s | 251 | 202 | 103 | 2.50 s | 1.87 s | 4.81 s | 6.78 | 282 | 199 | 120 | 2.83 s |
| `aggregate_time_bucket` | 7.07 s | 5.85 | 5.72 s | 652 | 101 | 60 | 977 | 6.19 s | 6.58 s | 6.68 | 697 | 104 | 93 | 960 |
| `join_lookup` | 2.69 | 11 | 0.06 | 2.36 | 1.78 | 3.44 | 0.59 | 0.08 | 4.10 | 12 | 1.82 | 3.05 | 3.47 | 0.70 |
| `graph_traversal` | 4.55 | 12 | 0.03 | 1.70 | 1.96 | 3.09 | 0.81 | 0.05 | 7.27 | 13 | 1.31 | 2.32 | 2.55 | 1.02 |
| `text_search` | 227 | 8.39 | 47 | 11 | 17 | 24 | 1.58 | 52 | 213 | 8.18 | 11 | 23 | 19 | 1.86 |
| `geo_near` | 657 | 13 | 154 | 1.98 | 18 | 16 | 1.10 | 174 | 661 | 13 | 1.64 | 18 | 25 | 1.15 |
| `vector_knn` | 4.82 | 12 | 54 | -- | 3.23 | 4.49 | 0.84 | 56 | 5.14 | 11 | -- | 4.72 | 4.90 | 1.01 |
| `transaction_transfer` | 5.48 | -- | 0.06 | 6.43 | 4.14 | 6.17 | 0.50 | 0.07 | 7.80 | -- | 4.72 | 4.53 | 7.25 | 0.41 |
| `ttl_set` | 1.27 | 6.66 | 0.04 | 4.47 | 4.87 | 8.76 | 0.40 | 0.03 | 1.75 | 6.99 | 1.99 | 5.37 | 7.09 | 0.39 |
| `scan_full` | 17.79 s | 8.85 | 747 | 578 | 2.91 | 3.45 | 1.16 s | 1.01 s | 17.75 s | 8.43 | 579 | 3.30 | 2.81 | 1.14 s |

## Feature probes

| feature | Cassandra 5.0 | Elasticsearch 9 | LMDB | MongoDB 7.0 | Neo4j 5 | Neo4j 5 Enterprise cluster | Redis 8 | RocksDB | Cassandra 5.0 (k3s) | Elasticsearch 9 (k3s) | MongoDB 7.0 (k3s) | Neo4j 5 (k3s) | Neo4j 5 Enterprise cluster (k3s) | Redis 8 (k3s) |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| secondary_index | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| compound_index | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ |
| unique_constraint | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ |
| ttl_expiry | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ✅ | ❌ | ✅ | ❌ | ✅ | ❌ | ❌ | ✅ |
| text_index | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ |
| geo_index | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ |
| vector_index | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ |
| partial_update | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| atomic_increment | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| optimistic_concurrency | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| multi_doc_transaction | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ |
| tunable_consistency | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| nested_query | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ |
| array_operators | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| aggregation | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ |
| server_side_join | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ |
| graph_traversal | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ |
| schema_validation | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ |
| change_streams | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ✅ | ❌ | ✅ | ❌ | ✅ | ❌ | ❌ | ✅ |
| server_side_scripting | ❌ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ |
| pubsub | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| explain_plan | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| bulk_import | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| large_document | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| many_fields | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| read_from_replica | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| snapshot_backup | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| incremental_backup | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| pitr | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ |
| slow_query_log | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| audit_log | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ |
| structured_logs | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ |
| runtime_log_level | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

## Optimisation experiments (speed-up of best variant vs first variant, p50)

| experiment | Cassandra 5.0 | Elasticsearch 9 | LMDB | MongoDB 7.0 | Neo4j 5 | Neo4j 5 Enterprise cluster | Redis 8 | RocksDB | Cassandra 5.0 (k3s) | Elasticsearch 9 (k3s) | MongoDB 7.0 (k3s) | Neo4j 5 (k3s) | Neo4j 5 Enterprise cluster (k3s) | Redis 8 (k3s) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `opt01_secondary_index` | x740.63 (query_table_((event_type,month),occurred_at)) | n/a ms | x13014.36 (ordered_index_(type,ts,id)) | x106.02 (index_(event_type,occurred_at)) | x6.34 (composite_(event_type,occurred_at)) | x4.24 (composite_(event_type,occurred_at)) | x6811.54 (index_(event_type,occurred_at)) | x10629.31 (ordered_index_(type,ts,id)) | x391.51 (query_table_((event_type,month),occurred_at)) | n/a ms | x115.26 (index_(event_type,occurred_at)) | x5.74 (composite_(event_type,occurred_at)) | x6.15 (composite_(event_type,occurred_at)) | x4445.44 (index_(occurred_at,event_type)) |
| `opt02_projection` | x1.69 (projection_3_fields) | x1.0 (full_document) | x1.24 (projection_3_fields) | x1.0 (full_document) | x1.05 (projection_3_fields) | x1.0 (projection_3_fields) | x1.08 (projection_3_fields) | x1.0 (full_document) | x1.0 (full_document) | x1.03 (projection_3_fields) | x1.0 (full_document) | x1.14 (projection_3_fields) | x1.31 (projection_3_fields) | x1.03 (projection_3_fields) |
| `opt03_pagination` | x31.62 (keyset_depth_10000) | x4.82 (keyset_depth_100000) | x14.42 (keyset_depth_10000) | x6.32 (keyset_depth_100000) | x2.24 (keyset_depth_100000) | x1.93 (keyset_depth_100000) | x1.0 (offset_depth_10000) | x10.95 (keyset_depth_100000) | x32.83 (keyset_depth_10000) | x4.14 (keyset_depth_10000) | x7.03 (keyset_depth_100000) | x2.12 (keyset_depth_100000) | x2.01 (keyset_depth_100000) | x1.0 (offset_depth_10000) |
| `opt04_batching` | x5.39 (bulk_put_1000_x2) | x51.22 (bulk_put_1000_x2) | x2.72 (bulk_put_1000_x2) | x86.21 (bulk_put_1000_x2) | x34.58 (bulk_put_1000_x2) | x53.63 (bulk_put_1000_x2) | x6.73 (bulk_put_1000_x2) | x1.83 (bulk_put_1000_x2) | x6.97 (bulk_put_1000_x2) | x54.11 (bulk_put_1000_x2) | x116.4 (bulk_put_1000_x2) | x43.72 (bulk_put_1000_x2) | x34.23 (bulk_put_1000_x2) | x13.93 (bulk_put_1000_x2) |
| `opt05_multi_get` | x7.41 (multi_get_100) | x17.36 (multi_get_100) | x1.09 (multi_get_100) | x38.98 (multi_get_100) | x17.08 (multi_get_100) | x28.31 (multi_get_100) | x13.56 (multi_get_100) | x1.66 (multi_get_100) | x8.46 (multi_get_100) | x18.49 (multi_get_100) | x54.02 (multi_get_100) | x17.71 (multi_get_100) | x23.46 (multi_get_100) | x10.35 (multi_get_100) |
| `opt06_embedding_vs_reference` | x1.0 (embedded_items) | x1.0 (embedded_items) | x1.0 (embedded_items) | x1.0 (embedded_items) | x1.01 (referenced_items) | x1.11 (referenced_items) | x1.0 (embedded_items) | x1.0 (embedded_items) | x1.0 (embedded_items) | x1.0 (embedded_items) | x1.0 (embedded_items) | x1.11 (referenced_items) | x1.04 (referenced_items) | x1.0 (embedded_items) |
| `opt07_write_durability` | x1.0 (ONE) | x1.0 (refresh_false) | n/a | x1.0 (w0_unacknowledged) | n/a | n/a | x1.0 (ack_primary) | n/a | x1.0 (ONE) | x1.0 (refresh_false) | x1.0 (w0_unacknowledged) | n/a | n/a | x1.0 (ack_primary) |
| `opt08_read_consistency` | x1.0 (ONE) | 3.238 ms | n/a | x1.37 (secondaryPreferred@replica) | n/a | n/a | x1.13 (replica@replica) | n/a | x1.02 (ALL) | 3.267 ms | x1.2 (secondaryPreferred@replica) | n/a | n/a | x1.0 (primary) |
| `opt09_connection_reuse` | x24.38 (persistent_client) | x2.43 (persistent_client) | x1.11 (persistent_client) | x9.79 (persistent_client) | x3.45 (persistent_client) | x7.68 (persistent_client) | x11.48 (persistent_client) | x1.23 (persistent_client) | x26.23 (persistent_client) | x2.28 (persistent_client) | x8.68 (persistent_client) | x2.61 (persistent_client) | x6.72 (persistent_client) | x8.34 (persistent_client) |

## Connections

| engine | max conn | connect p50 ms | storm 200: opened/failed | 1 worker ops/s | 32 workers ops/s | 32w p99 ms | 64 workers ops/s | CAS contention (applied/rejected, atomic) |
|---|---:|---:|---|---:|---:|---:|---:|---|
| Cassandra 5.0 | None | 30 | 200/0 | 625 | 16531 | 3.81 | 15714 | 146/54 atomic |
| Elasticsearch 9 | None | 3.08 | - | 330 | 11514 | 5.57 | 12493 | 98/102 NOT atomic |
| LMDB | None | 0.00 | - | 98712 | 28948 | 32 | 29338 | 200/0 NOT atomic |
| MongoDB 7.0 | 5000 | 3.90 | 200/0 | 786 | 27956 | 2.31 | 30570 | 96/104 atomic |
| Neo4j 5 | 800 | 5.92 | 200/0 | 520 | 13555 | 4.91 | 15302 | 200/0 atomic |
| Neo4j 5 Enterprise cluster | 800 | 18 | 200/0 | 447 | 8521 | 7.50 | 7606 | 200/0 atomic |
| Redis 8 | 10000 | 2.22 | 200/0 | 2186 | 68425 | 0.95 | 79313 | 69/131 atomic |
| RocksDB | None | 0.00 | - | 74229 | 35889 | 0.07 | 34356 | 200/0 atomic |
| Cassandra 5.0 (k3s) | None | 29 | 200/0 | 426 | 16762 | 3.20 | 15310 | 134/66 atomic |
| Elasticsearch 9 (k3s) | None | 6.47 | - | 308 | 11565 | 5.29 | 11716 | 72/128 NOT atomic |
| MongoDB 7.0 (k3s) | 409 | 4.13 | 110/90 | 898 | 23781 | 2.79 | 24361 | 89/111 atomic |
| Neo4j 5 (k3s) | 800 | 7.08 | 200/0 | 457 | 11969 | 5.24 | 12002 | 200/0 atomic |
| Neo4j 5 Enterprise cluster (k3s) | 800 | 17 | 200/0 | 515 | 7484 | 11 | 8613 | 200/0 atomic |
| Redis 8 (k3s) | 10000 | 2.96 | 200/0 | 1895 | 58733 | 1.06 | 67202 | 69/131 atomic |

## Replication / scaling

| engine | kind | visibility lag p50 / p95 / max ms | replica rejects writes | catch-up after 20k docs (ms) | read scaling gain | failover downtime ms |
|---|---|---|---|---|---:|---:|
| Cassandra 5.0 | leaderless replication, RF=3, tunable consistency (LOCAL_QUORUM) | - | no | 489.0 | 0.73 | 900.5 |
| Elasticsearch 9 | primary + replica shards (number_of_replicas 2) over 3 nodes; master election by quorum | 6.42 / 20 / 27 | no | 51.4 | 1.24 | 4869.9 |
| LMDB | none | single-node | - | - | - | - |
| MongoDB 7.0 | replica set (oplog, async, automatic election) | 1.35 / 1.47 / 1.52 | yes  | 28.9 | 1.01 | 30503.8 |
| Neo4j 5 | none (single instance; Enterprise causal clustering not in Community) | single-node | - | - | - | - |
| Neo4j 5 Enterprise cluster | Raft cluster of 3 primaries (leader takes writes, followers replicate; automatic leader election) | 8.04 / 16 / 96 | yes  | 18.1 | 1.24 | 20900.1 |
| Redis 8 | async primary/replica (diskless sync, AOF everysec) + Sentinel failover | 0.74 / 0.94 / 45 | yes READONLY | 1.4 | 1.02 | 2500.1 |
| RocksDB | none | single-node | - | - | - | - |
| Cassandra 5.0 (k3s) | leaderless replication, RF=3, tunable consistency (LOCAL_QUORUM) | 3.52 / 3.95 / 12 | no | 482.0 | 0.82 | 1612.5 |
| Elasticsearch 9 (k3s) | primary + replica shards (number_of_replicas 2) over 3 nodes; master election by quorum | 8.88 / 27 / 35 | no | 21.2 | 1.18 | 1045.7 |
| MongoDB 7.0 (k3s) | replica set (oplog, async, automatic election) | 1.29 / 1.56 / 1.70 | yes  | 27.7 | 1.01 | 923.2 |
| Neo4j 5 (k3s) | none (single instance; Enterprise causal clustering not in Community) | single-node | - | - | - | - |
| Neo4j 5 Enterprise cluster (k3s) | Raft cluster of 3 primaries (leader takes writes, followers replicate; automatic leader election) | 7.25 / 14 / 57 | yes  | 28.7 | 0.79 | 11257.4 |
| Redis 8 (k3s) | async primary/replica (diskless sync, AOF everysec) + Sentinel failover | 0.79 / 0.94 / 2.43 | yes READONLY | 7.5 | 1.01 | 3181.1 |

## Multi-connection bulk-insert load test (docs/s; batch size and duration per engine in latest.json)

| engine | entry point | connections | docs/s | batch p50 ms | errors | busiest container (avg cores during inserts) |
|---|---|---:|---:|---:|---:|---|
| Cassandra 5.0 | primary | 4 | 15200 | 244 | 0 | cassandra1 8.18 |
| Cassandra 5.0 | primary | 16 | 31467 | 496 | 0 | cassandra1 11.73 |
| Cassandra 5.0 | primary | 32 | 33667 | 938 | 0 | cassandra1 12.26 |
| Elasticsearch 9 | primary | 4 | 40200 | 97 | 0 | es1 3.22 |
| Elasticsearch 9 | primary | 16 | 111533 | 102 | 0 | es1 10.95 |
| Elasticsearch 9 | primary | 32 | 106333 | 228 | 0 | es1 12.73 |
| MongoDB 7.0 | primary | 4 | 83800 | 31 | 0 | mongo2 13.77 |
| MongoDB 7.0 | primary | 16 | 135600 | 106 | 0 | mongo2 25.25 |
| MongoDB 7.0 | primary | 32 | 156000 | 195 | 0 | mongo2 30.36 |
| Neo4j 5 | primary | 4 | 14067 | 135 | 0 | neo4j 3.94 |
| Neo4j 5 | primary | 16 | 11533 | 208 | 0 | neo4j 4.52 |
| Neo4j 5 | primary | 32 | 14600 | 207 | 1 | neo4j 5.81 |
| Neo4j 5 Enterprise cluster | primary | 4 | 11067 | 167 | 0 | neo4j3 3.73 |
| Neo4j 5 Enterprise cluster | primary | 16 | 9267 | 307 | 0 | neo4j3 4.50 |
| Neo4j 5 Enterprise cluster | primary | 32 | 9600 | 1.42 s | 0 | neo4j3 5.04 |
| Redis 8 | primary | 4 | 61800 | 59 | 0 | redis-primary 3.41 |
| Redis 8 | primary | 16 | 89733 | 177 | 0 | redis-primary 6.42 |
| Redis 8 | primary | 32 | 89867 | 352 | 0 | redis-primary 6.07 |
| Redis 8 | haproxy-ro | 4 | 44733 | 60 | 21 | redis-primary 3.12 |
| Redis 8 | haproxy-ro | 16 | 81800 | 94 | 168 | redis-primary 4.64 |
| Redis 8 | haproxy-ro | 32 | 78200 | 192 | 336 | redis-primary 5.92 |
| Cassandra 5.0 (k3s) | primary | 4 | 13200 | 250 | 0 | - |
| Cassandra 5.0 (k3s) | primary | 16 | 28267 | 430 | 0 | - |
| Cassandra 5.0 (k3s) | primary | 32 | 23667 | 970 | 0 | - |
| Elasticsearch 9 (k3s) | primary | 4 | 36000 | 107 | 0 | - |
| Elasticsearch 9 (k3s) | primary | 16 | 120200 | 102 | 0 | - |
| Elasticsearch 9 (k3s) | primary | 32 | 111800 | 207 | 0 | - |
| MongoDB 7.0 (k3s) | primary | 4 | 103400 | 32 | 0 | - |
| MongoDB 7.0 (k3s) | primary | 16 | 117467 | 113 | 0 | - |
| MongoDB 7.0 (k3s) | primary | 32 | 138267 | 206 | 0 | - |
| Neo4j 5 (k3s) | primary | 4 | 14200 | 137 | 0 | - |
| Neo4j 5 (k3s) | primary | 16 | 14000 | 198 | 0 | - |
| Neo4j 5 (k3s) | primary | 32 | 11267 | 290 | 2 | - |
| Neo4j 5 Enterprise cluster (k3s) | primary | 4 | 12467 | 156 | 0 | - |
| Neo4j 5 Enterprise cluster (k3s) | primary | 16 | 11733 | 213 | 0 | - |
| Neo4j 5 Enterprise cluster (k3s) | primary | 32 | 11267 | 374 | 1 | - |
| Redis 8 (k3s) | primary | 4 | 63267 | 57 | 0 | - |
| Redis 8 (k3s) | primary | 16 | 86533 | 184 | 0 | - |
| Redis 8 (k3s) | primary | 32 | 89600 | 353 | 0 | - |

## Backup & restore (one full backup with the engine's own tooling, then a verified restore)

| engine | backup method | online | backup s | artifact | % of data | point-read p50 during / baseline ms | restore method | restore s | downtime ms | verified | incremental | DR drill (export → down -v → fresh up → import → restore) |
|---|---|:---:|---:|---:|---:|---|---|---:|---:|:---:|---|---|
| Cassandra 5.0 | nodetool flush + snapshot --ttl 3h on every node, export to /backups (sstableloader layout) | yes | 8.56 | 379.5 MB | 100.0 | 1.42 / 1.63 | TRUNCATE tables + nodetool import --copy-data of each node's own snapshot export (parallel per node) | 21.38 | 0.00 | ✅ | 11 KB in 3.15 s | ✅ verified — export 378.6 MB, fresh up 55.5 s, restore 39.3 s |
| Elasticsearch 9 | PUT _snapshot/nslab/<tag> (fs repository, compress) | yes | 7.16 | 245.9 MB | 41.6 | 2.82 / 2.85 | DELETE the 8 lab_* indices (by name) + POST _snapshot/nslab/<tag>/_restore | 6.94 | 0.00 | ✅ | 66 KB in 0.15 s | ✅ verified — export 166.5 MB, fresh up 37.0 s, restore 3.8 s |
| LMDB | lmdb env.copy(compact=True) inside a read txn | yes | 3.35 | 828.9 MB | 100.0 | 0.07 / 0.06 | close env; copy backup file over lab.mdb; reopen | 6.79 | 6.79 s | ✅ | unsupported | ✅ verified — export 828.9 MB, fresh up 0.0 s, restore 3.33 s |
| MongoDB 7.0 | mongodump --oplog --gzip --archive | yes | 13.55 | 50.8 MB | 27.1 | 0.95 / 1.01 | dropDatabase + mongorestore --oplogReplay --drop | 59.53 | 0.00 | ✅ | unsupported | ✅ verified — export 50.8 MB, fresh up 22.1 s, restore 67.55 s |
| Neo4j 5 | stop server; neo4j-admin database dump; start (Community: offline) | no (33570.3 ms down) | 33.79 | 306.2 MB | 16.7 | 3.60 / 4.17 | stop server; neo4j-admin database load --overwrite-destination; start | 42.16 | 42.08 s | ✅ | unsupported | ✅ verified — export 269.2 MB, fresh up 17.3 s, restore 37.83 s |
| Neo4j 5 Enterprise cluster | neo4j-admin database backup lab --from=neo4j1:6362 --type=full (online, Enterprise) | yes | 19.75 | 92.3 MB | 5.4 | 2.13 / 2.70 | DROP DATABASE + CREATE DATABASE ... OPTIONS {existingData:'use', seedURI:'file://.../<tag>.backup'} WAIT (every primary seeds from the shared file) | 15.88 | 15.81 s | ✅ | unsupported | ✅ verified — export 92.3 MB, fresh up 45.3 s, restore 19.38 s |
| Redis 8 | BGSAVE + copy dump.rdb | yes | 5.78 | 335.8 MB | 77.3 | 0.59 / 0.54 | stop; RDB -> appendonlydir base + manifest; start; replicas full-resync | 24.01 | 11.37 s | ✅ | unsupported | ✅ verified — export 335.8 MB, fresh up 12.6 s, restore 23.17 s |
| RocksDB | rocksdict Checkpoint.create_checkpoint (hard links) after flush | yes | 0.03 | 170.8 MB | 50.4 | 0.06 / 0.09 | close; replace lab.rocks with the checkpoint dir; reopen | 0.1 | 104 | ✅ | unsupported | ✅ verified — export 170.8 MB, fresh up 0.0 s, restore 0.09 s |
| Cassandra 5.0 (k3s) | nodetool flush + snapshot --ttl 3h on every node, export to /backups (sstableloader layout) | yes | 11.72 | 509.7 MB | 106.6 | 1.86 / 1.82 | TRUNCATE tables + nodetool import --copy-data of each node's own snapshot export (parallel per node) | 27.8 | 0.00 | ✅ | 5 KB in 3.71 s | ✅ verified — export 378.5 MB, fresh up 62.1 s, restore 44.67 s |
| Elasticsearch 9 (k3s) | PUT _snapshot/nslab/<tag> (fs repository, compress) | yes | 7.74 | 310.3 MB | 54.3 | 2.97 / 2.95 | DELETE the 8 lab_* indices (by name) + POST _snapshot/nslab/<tag>/_restore | 10.39 | 0.00 | ✅ | 10 KB in 0.48 s | ✅ verified — export 166.7 MB, fresh up 39.3 s, restore 3.88 s |
| MongoDB 7.0 (k3s) | mongodump --oplog --gzip --archive | yes | 15.86 | 50.8 MB | 4.7 | 1.23 / 1.24 | dropDatabase + mongorestore --oplogReplay --drop | 76.0 | 0.00 | ✅ | unsupported | ✅ verified — export 50.8 MB, fresh up 31.1 s, restore 68.84 s |
| Neo4j 5 (k3s) | stop server; neo4j-admin database dump; start (Community: offline) | no (52271.9 ms down) | 53.24 | 394.4 MB | 16.6 | 1.82 / 2.35 | stop server; neo4j-admin database load --overwrite-destination; start | 63.56 | 71.57 s | ✅ | unsupported | ✅ verified — export 269.2 MB, fresh up 27.9 s, restore 52.92 s |
| Neo4j 5 Enterprise cluster (k3s) | neo4j-admin database backup lab --from=neo4j1:6362 --type=full (online, Enterprise) | yes | 25.0 | 91.7 MB | 5.7 | 2.45 / 2.84 | DROP DATABASE + CREATE DATABASE ... OPTIONS {existingData:'use', seedURI:'file://.../<tag>.backup'} WAIT (every primary seeds from the shared file) | 16.41 | 16.17 s | ✅ | unsupported | ✅ verified — export 91.7 MB, fresh up 54.4 s, restore 15.48 s |
| Redis 8 (k3s) | BGSAVE + copy dump.rdb | yes | 7.26 | 411.2 MB | 25.7 | 0.69 / 0.73 | stop; RDB -> appendonlydir base + manifest; start; replicas full-resync | 84.29 | 20.09 s | ✅ | unsupported | ✅ verified — export 335.8 MB, fresh up 22.5 s, restore 57.31 s |

Strategy per engine (tool · consistency · restore · incremental · PITR · schedule · retention): see `phases.backup.strategy` in each `latest.json` and `docs/backup-logging.md`.

## Logging (container log driver, server log format, slow-query log, audit log, run-time log level)

| engine | container logs (driver / rotation / logged so far) | server log | slow-query log | slow op ms → logged as | audit log | run-time level change |
|---|---|---|---|---|---|---|
| Cassandra 5.0 | json-file 50mx3 / text / 4.3 MB | text @ /opt/cassandra/logs/{system,debug,gc}.log + stdout | ✅ 500 ms threshold | 4155.7 → 1736 | ✅ 3 entries | ✅ nodetool setlogginglevel org.apache.cassandra.db.Keyspace TRACE -> ver |
| Elasticsearch 9 | json-file 50mx3 / json / 347 KB | json (ECS) @ stdout (docker logs); logs/ dir when running from  | ✅ 100 ms threshold | 554.8 → 466.0 | ❌ xpack.security.audit.enabled needs xpack.security enabled (o | ✅ PUT _cluster/settings transient logger.org.elasticsearch.snapshots=DEB |
| LMDB | n/a (embedded) /  / 0 B | none @ in-process library: log in the host application | ❌ no server: time the calls in the application (decorator / Op | - | ❌ no server: audit in the application | ❌ no server log level (RocksDB info_log_level is an open-time  |
| MongoDB 7.0 | json-file 50mx3 / json / 23.2 MB | json @ stdout (docker logs) — systemLog.path when set; ge | ✅ 100 ms threshold | 1099.0 → 1097 | ❌ auditLog is MongoDB Enterprise / Percona Server for MongoDB  | ✅ setParameter logComponentVerbosity.command -1 -> 1 -> -1 (no restart;  |
| Neo4j 5 | json-file 50mx3 / text / 7 KB | text (log4j2 PatternLayout; JsonTemplateLayout available) @ /logs/{neo4j,debug,query,security,http}.log + stdo | ✅ 100 ms threshold | 924.6 → 722.8 | ❌ security.log (authentication/authorization events) and query | ✅ edited conf/server-logs.xml Root level INFO->DEBUG; log4j2 monitorInte |
| Neo4j 5 Enterprise cluster | json-file 50mx3 / text / 8 KB | text (log4j2 PatternLayout; JsonTemplateLayout available) @ /logs/{neo4j,debug,query,security,http}.log + stdo | ✅ 100 ms threshold | 361.4 → 359 | ✅ 1 entries | ✅ CALL dbms.setConfigValue('db.logs.query.threshold', '250ms') -> SHOW S |
| Redis 8 | json-file 50mx3 / text / 95 KB | text @ stdout (docker logs) — `logfile` when set | ✅ 100 ms threshold | 226.4 → 220.67 | ✅ 2 entries | ✅ CONFIG SET loglevel debug -> verified -> back to notice (no restart) |
| RocksDB | n/a (embedded) /  / 0 B | text (RocksDB info LOG in the DB dir) @ data/rocksdb/lab.rocks/LOG | ❌ no server: time the calls in the application (decorator / Op | - | ❌ no server: audit in the application | ❌ no server log level (RocksDB info_log_level is an open-time  |
| Cassandra 5.0 (k3s) | kubelet (container-log-max-size/-files via k3d.yaml) 50Mix3 / text / 1.6 MB | text @ /opt/cassandra/logs/{system,debug,gc}.log + stdout | ✅ 500 ms threshold | 3609.1 → 1342 | ✅ 3 entries | ✅ nodetool setlogginglevel org.apache.cassandra.db.Keyspace TRACE -> ver |
| Elasticsearch 9 (k3s) | kubelet (container-log-max-size/-files via k3d.yaml) 50Mix3 / json / 376 KB | json (ECS) @ stdout (docker logs); logs/ dir when running from  | ✅ 100 ms threshold | 667.0 → 562.0 | ❌ xpack.security.audit.enabled needs xpack.security enabled (o | ✅ PUT _cluster/settings transient logger.org.elasticsearch.snapshots=DEB |
| MongoDB 7.0 (k3s) | kubelet (container-log-max-size/-files via k3d.yaml) 50Mix3 / json / 28.5 MB | json @ stdout (docker logs) — systemLog.path when set; ge | ✅ 100 ms threshold | 1298.4 → 1297 | ❌ auditLog is MongoDB Enterprise / Percona Server for MongoDB  | ✅ setParameter logComponentVerbosity.command -1 -> 1 -> -1 (no restart;  |
| Neo4j 5 (k3s) | kubelet (container-log-max-size/-files via k3d.yaml) 50Mix3 / text / 1 KB | text (log4j2 PatternLayout; JsonTemplateLayout available) @ /logs/{neo4j,debug,query,security,http}.log + stdo | ✅ 100 ms threshold | 644.7 → 571.1 | ❌ security.log (authentication/authorization events) and query | ✅ edited conf/server-logs.xml Root level INFO->DEBUG; log4j2 monitorInte |
| Neo4j 5 Enterprise cluster (k3s) | kubelet (container-log-max-size/-files via k3d.yaml) 50Mix3 / text / 8 KB | text (log4j2 PatternLayout; JsonTemplateLayout available) @ /logs/{neo4j,debug,query,security,http}.log + stdo | ✅ 100 ms threshold | 427.5 → 424 | ✅ 3 entries | ❌ NotALeader: {neo4j_code: Neo.ClientError.Cluster.NotALeader} |
| Redis 8 (k3s) | kubelet (container-log-max-size/-files via k3d.yaml) 50Mix3 / text / 52 KB | text @ stdout (docker logs) — `logfile` when set | ✅ 100 ms threshold | 232.2 → 220.71 | ✅ 2 entries | ✅ CONFIG SET loglevel debug -> verified -> back to notice (no restart) |

## Notes per engine

- **Cassandra 5.0**: Official cassandra:5 image, 3 nodes RF=3, LOCAL_QUORUM by default. Query-first schema (one table per access path), SAI indexes for vector (ANN) and analyzed text search, LWT for compare-and-set, counter table for increments. Nodes are addressed by static container IPs (10.99.1.11-13); the client is pinned to one node per target. 
- **Elasticsearch 9**: Elasticsearch 9, 3-node cluster (all master-eligible + data), security disabled. One index per collection with 1 primary shard + 2 replicas, so every node holds every shard (the RF=3 equivalent of the other clustered stacks). Every field indexed at mapping time (inverted index for keyword/text, BKD tree for numeric/geo, HNSW for dense_vector). Eventually consistent for search (30 s refresh; the harness refreshes after load). No multi-document transactions, no server-side joins across indices, no per-document TTL. Any node coordinates writes; the master is re-elected by the remaining quorum when a node dies. 
- **LMDB**: Lightning Memory-Mapped Database, in-process, single writer / many MVCC readers. No query engine: an ordered key-value map. The lab models documents plus hand-maintained ordered index keyspaces (idx/...); aggregations, text, geo and vector scan the primary keyspace. 8 GB map. No server, no network, no replication. 
- **MongoDB 7.0**: Official mongo:7 image (7.0.x). mongo:8 (8.3.9) refuses to start on this host's Linux 7.0.0 kernel (SERVER-121912: TCMalloc rseq incompatibility with kernels 6.19-7.0.13, exits at startup, no bypass) so the lab pins 7.0. 3-member replica set rs0 (priorities 3/2/1) initiated by a one-shot mongosh container; no auth. Per-node targets use directConnection=true; writes w=1 unless an experiment says otherwise. $vectorSearch is Atlas / mongot-only, so vector_knn is unsupported on Community. 
- **Neo4j 5**: Neo4j 5 Community, single instance (causal clustering, CDC, online backup and read routing are Enterprise-only). Order items are CONTAINS relationships, categories a CHILD_OF tree, so graph_traversal and join_lookup are native. Built-in full-text (Lucene) and HNSW vector indexes; no native TTL or pub/sub. 
- **Neo4j 5 Enterprise cluster**: Neo4j 5 Enterprise (evaluation licence) with 3 primaries in one Raft cluster: the elected leader takes writes, followers replicate and reject direct writes, a new leader is elected when the leader dies. The `primary` target is a routing URI (neo4j://) so writes always reach the leader; node2/node3 are direct Bolt connections to individual members. The dataset lives in a database called `lab` (created by the harness) so it can be dropped and re-seeded from an online backup (neo4j-admin database backup + CREATE DATABASE ... seedURI). Same graph model and indexes as the Community stack. Licence: Enterprise features are used under Neo4j's evaluation terms only. 
- **Redis 8**: Redis Open Source 8 (RediSearch/JSON/TimeSeries/probabilistic modules built in). Async primary->replica replication with diskless sync, AOF everysec on both; 3 Sentinels (quorum 2, down-after 2 s) elect a new primary; HAProxy round-robins reads over primary+replica. Documents are hashes with application-maintained index sets/zsets; FT.CREATE on products for text + vector. 
- **RocksDB**: RocksDB (Facebook LSM-tree) via rocksdict, in-process, thread-safe. Same modelling as LMDB (ordered key-value + hand-maintained index keyspaces, WriteBatch for atomic multi-key writes). Write-optimised (LSM) vs LMDB's read-optimised B+tree. No server, no network, no replication. 
- **Cassandra 5.0 (k3s)**: Official cassandra:5 image, 3 nodes RF=3, LOCAL_QUORUM by default. Query-first schema (one table per access path), SAI indexes for vector (ANN) and analyzed text search, LWT for compare-and-set, counter table for increments. Nodes are addressed by static container IPs (10.99.1.11-13); the client is pinned to one node per target. 
- **Elasticsearch 9 (k3s)**: Elasticsearch 9, 3-node cluster (all master-eligible + data), security disabled. One index per collection with 1 primary shard + 2 replicas, so every node holds every shard (the RF=3 equivalent of the other clustered stacks). Every field indexed at mapping time (inverted index for keyword/text, BKD tree for numeric/geo, HNSW for dense_vector). Eventually consistent for search (30 s refresh; the harness refreshes after load). No multi-document transactions, no server-side joins across indices, no per-document TTL. Any node coordinates writes; the master is re-elected by the remaining quorum when a node dies. 
- **MongoDB 7.0 (k3s)**: Official mongo:7 image (7.0.x). mongo:8 (8.3.9) refuses to start on this host's Linux 7.0.0 kernel (SERVER-121912: TCMalloc rseq incompatibility with kernels 6.19-7.0.13, exits at startup, no bypass) so the lab pins 7.0. 3-member replica set rs0 (priorities 3/2/1) initiated by a one-shot mongosh container; no auth. Per-node targets use directConnection=true; writes w=1 unless an experiment says otherwise. $vectorSearch is Atlas / mongot-only, so vector_knn is unsupported on Community. 
- **Neo4j 5 (k3s)**: Neo4j 5 Community, single instance (causal clustering, CDC, online backup and read routing are Enterprise-only). Order items are CONTAINS relationships, categories a CHILD_OF tree, so graph_traversal and join_lookup are native. Built-in full-text (Lucene) and HNSW vector indexes; no native TTL or pub/sub. 
- **Neo4j 5 Enterprise cluster (k3s)**: Neo4j 5 Enterprise (evaluation licence) with 3 primaries in one Raft cluster: the elected leader takes writes, followers replicate and reject direct writes, a new leader is elected when the leader dies. The `primary` target is a routing URI (neo4j://) so writes always reach the leader; node2/node3 are direct Bolt connections to individual members. The dataset lives in a database called `lab` (created by the harness) so it can be dropped and re-seeded from an online backup (neo4j-admin database backup + CREATE DATABASE ... seedURI). Same graph model and indexes as the Community stack. Licence: Enterprise features are used under Neo4j's evaluation terms only. 
- **Redis 8 (k3s)**: Redis Open Source 8 (RediSearch/JSON/TimeSeries/probabilistic modules built in). Async primary->replica replication with diskless sync, AOF everysec on both; 3 Sentinels (quorum 2, down-after 2 s) elect a new primary; HAProxy round-robins reads over primary+replica. Documents are hashes with application-maintained index sets/zsets; FT.CREATE on products for text + vector. 
