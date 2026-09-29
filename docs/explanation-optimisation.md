# Optimisation experiments

Each experiment measures an operation, changes one thing (index, document shape, batching, consistency level,
connection handling), and measures again. Speed-up is best variant vs first variant.

## opt01_secondary_index

**Range scan on events(event_type, occurred_at): no index vs secondary index (both column orders)**

_an access path that is not indexed is a full scan; composite order matters on B-tree-like engines_

### MongoDB 7.0

Speed-up: **×106.02** (best: index_(event_type,occurred_at))

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| no_index | 594 ms | — | ✔ |  |
| index_(event_type,occurred_at) | 5.60 ms | — | ✔ |  |
| index_(occurred_at,event_type) | 16 ms | — | ✔ |  |

### MongoDB 7.0 (k3s)

Speed-up: **×115.26** (best: index_(event_type,occurred_at))

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| no_index | 633 ms | — | ✔ |  |
| index_(event_type,occurred_at) | 5.49 ms | — | ✔ |  |
| index_(occurred_at,event_type) | 16 ms | — | ✔ |  |

### LMDB

Speed-up: **×13014.36** (best: ordered_index_(type,ts,id))

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| no_index_scan | 5.49 s | — | ✔ |  |
| ordered_index_(type,ts,id) | 0.42 ms | — | ✔ |  |

### RocksDB

Speed-up: **×10629.31** (best: ordered_index_(type,ts,id))

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| no_index_scan | 7.05 s | — | ✔ |  |
| ordered_index_(type,ts,id) | 0.66 ms | — | ✔ |  |

### Neo4j 5

Speed-up: **×6.34** (best: composite_(event_type,occurred_at))

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| no_index | 79 ms | — | ✔ |  |
| composite_(event_type,occurred_at) | 12 ms | — | ✔ |  |

### Neo4j 5 (k3s)

Speed-up: **×5.74** (best: composite_(event_type,occurred_at))

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| no_index | 81 ms | — | ✔ |  |
| composite_(event_type,occurred_at) | 14 ms | — | ✔ |  |

### Neo4j 5 Enterprise cluster

Speed-up: **×4.24** (best: composite_(event_type,occurred_at))

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| no_index | 54 ms | — | ✔ |  |
| composite_(event_type,occurred_at) | 13 ms | — | ✔ |  |

### Neo4j 5 Enterprise cluster (k3s)

Speed-up: **×6.15** (best: composite_(event_type,occurred_at))

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| no_index | 55 ms | — | ✔ |  |
| composite_(event_type,occurred_at) | 8.99 ms | — | ✔ |  |

### Redis 8

Speed-up: **×6811.54** (best: index_(event_type,occurred_at))

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| no_index | 13.37 s | — | ✔ |  |
| index_(event_type,occurred_at) | 1.96 ms | — | ✔ |  |
| index_(occurred_at,event_type) | 3.30 ms | — | ✔ |  |

### Redis 8 (k3s)

Speed-up: **×4445.44** (best: index_(occurred_at,event_type))

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| no_index | 14.17 s | — | ✔ |  |
| index_(event_type,occurred_at) | 3.87 ms | — | ✔ |  |
| index_(occurred_at,event_type) | 3.19 ms | — | ✔ |  |

### Elasticsearch 9


| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| indexed_field (all fields indexed by default) | — | — | ✘ |  |

### Elasticsearch 9 (k3s)


| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| indexed_field (all fields indexed by default) | — | — | ✘ |  |

### Cassandra 5.0

Speed-up: **×740.63** (best: query_table_((event_type,month),occurred_at))

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| no_index_allow_filtering | 4.79 s | — | ✔ |  |
| sai_index_(event_type,occurred_at) | — | — | ✘ |  |
| query_table_((event_type,month),occurred_at) | 6.47 ms | — | ✔ |  |

### Cassandra 5.0 (k3s)

Speed-up: **×391.51** (best: query_table_((event_type,month),occurred_at))

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| no_index_allow_filtering | 4.81 s | — | ✔ |  |
| sai_index_(event_type,occurred_at) | — | — | ✘ |  |
| query_table_((event_type,month),occurred_at) | 12 ms | — | ✔ |  |

## opt02_projection

**Point read of an order: whole document (with embedded items) vs 3-field projection**

_returning fewer fields saves serialisation and transfer; some engines still read the whole document_

### MongoDB 7.0

Speed-up: **×1.0** (best: full_document)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| full_document | 1.31 ms | — | ✔ |  |
| projection_3_fields | 1.41 ms | — | ✔ |  |

### MongoDB 7.0 (k3s)

Speed-up: **×1.0** (best: full_document)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| full_document | 1.21 ms | — | ✔ |  |
| projection_3_fields | 1.33 ms | — | ✔ |  |

### LMDB

Speed-up: **×1.24** (best: projection_3_fields)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| full_document | 0.02 ms | — | ✔ |  |
| projection_3_fields | 0.02 ms | — | ✔ |  |

### RocksDB

Speed-up: **×1.0** (best: full_document)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| full_document | 0.03 ms | — | ✔ |  |
| projection_3_fields | 0.03 ms | — | ✔ |  |

### Neo4j 5

Speed-up: **×1.05** (best: projection_3_fields)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| full_document | 3.26 ms | — | ✔ |  |
| projection_3_fields | 3.11 ms | — | ✔ |  |

### Neo4j 5 (k3s)

Speed-up: **×1.14** (best: projection_3_fields)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| full_document | 3.60 ms | — | ✔ |  |
| projection_3_fields | 3.17 ms | — | ✔ |  |

### Neo4j 5 Enterprise cluster

Speed-up: **×1.0** (best: projection_3_fields)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| full_document | 2.66 ms | — | ✔ |  |
| projection_3_fields | 2.64 ms | — | ✔ |  |

### Neo4j 5 Enterprise cluster (k3s)

Speed-up: **×1.31** (best: projection_3_fields)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| full_document | 2.68 ms | — | ✔ |  |
| projection_3_fields | 2.05 ms | — | ✔ |  |

### Redis 8

Speed-up: **×1.08** (best: projection_3_fields)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| full_document | 0.26 ms | — | ✔ |  |
| projection_3_fields | 0.24 ms | — | ✔ |  |

### Redis 8 (k3s)

Speed-up: **×1.03** (best: projection_3_fields)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| full_document | 0.31 ms | — | ✔ |  |
| projection_3_fields | 0.30 ms | — | ✔ |  |

### Elasticsearch 9

Speed-up: **×1.0** (best: full_document)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| full_document | 3.31 ms | — | ✔ |  |
| projection_3_fields | 4.43 ms | — | ✔ |  |

### Elasticsearch 9 (k3s)

Speed-up: **×1.03** (best: projection_3_fields)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| full_document | 3.63 ms | — | ✔ |  |
| projection_3_fields | 3.51 ms | — | ✔ |  |

### Cassandra 5.0

Speed-up: **×1.69** (best: projection_3_fields)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| full_document | 1.78 ms | — | ✔ |  |
| projection_3_fields | 1.05 ms | — | ✔ |  |

### Cassandra 5.0 (k3s)

Speed-up: **×1.0** (best: full_document)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| full_document | 2.14 ms | — | ✔ |  |
| projection_3_fields | 2.27 ms | — | ✔ |  |

## opt03_pagination

**Deep pagination: offset/skip vs keyset (cursor) at depth 10k and 100k**

_offset still walks and discards rows; a keyset cursor seeks directly into the sorted index_

### MongoDB 7.0

Speed-up: **×6.32** (best: keyset_depth_100000)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| offset_depth_10000 | 12 ms | — | ✔ |  |
| keyset_depth_10000 | 2.15 ms | — | ✔ |  |
| offset_depth_100000 | 61 ms | — | ✔ |  |
| keyset_depth_100000 | 1.98 ms | — | ✔ |  |

### MongoDB 7.0 (k3s)

Speed-up: **×7.03** (best: keyset_depth_100000)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| offset_depth_10000 | 12 ms | — | ✔ |  |
| keyset_depth_10000 | 1.86 ms | — | ✔ |  |
| offset_depth_100000 | 68 ms | — | ✔ |  |
| keyset_depth_100000 | 1.78 ms | — | ✔ |  |

### LMDB

Speed-up: **×14.42** (best: keyset_depth_10000)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| offset_depth_10000 | 8.00 ms | — | ✔ |  |
| keyset_depth_10000 | 0.56 ms | — | ✔ |  |
| offset_depth_100000 | 77 ms | — | ✔ |  |
| keyset_depth_100000 | 0.60 ms | — | ✔ |  |

### RocksDB

Speed-up: **×10.95** (best: keyset_depth_100000)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| offset_depth_10000 | 8.12 ms | — | ✔ |  |
| keyset_depth_10000 | 1.42 ms | — | ✔ |  |
| offset_depth_100000 | 76 ms | — | ✔ |  |
| keyset_depth_100000 | 0.74 ms | — | ✔ |  |

### Neo4j 5

Speed-up: **×2.24** (best: keyset_depth_100000)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| offset_depth_10000 | 278 ms | — | ✔ |  |
| keyset_depth_10000 | 232 ms | — | ✔ |  |
| offset_depth_100000 | 651 ms | — | ✔ |  |
| keyset_depth_100000 | 124 ms | — | ✔ |  |

### Neo4j 5 (k3s)

Speed-up: **×2.12** (best: keyset_depth_100000)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| offset_depth_10000 | 272 ms | — | ✔ |  |
| keyset_depth_10000 | 245 ms | — | ✔ |  |
| offset_depth_100000 | 650 ms | — | ✔ |  |
| keyset_depth_100000 | 128 ms | — | ✔ |  |

### Neo4j 5 Enterprise cluster

Speed-up: **×1.93** (best: keyset_depth_100000)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| offset_depth_10000 | 200 ms | — | ✔ |  |
| keyset_depth_10000 | 196 ms | — | ✔ |  |
| offset_depth_100000 | 687 ms | — | ✔ |  |
| keyset_depth_100000 | 103 ms | — | ✔ |  |

### Neo4j 5 Enterprise cluster (k3s)

Speed-up: **×2.01** (best: keyset_depth_100000)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| offset_depth_10000 | 234 ms | — | ✔ |  |
| keyset_depth_10000 | 218 ms | — | ✔ |  |
| offset_depth_100000 | 876 ms | — | ✔ |  |
| keyset_depth_100000 | 117 ms | — | ✔ |  |

### Redis 8

Speed-up: **×1.0** (best: offset_depth_10000)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| offset_depth_10000 | 2.04 ms | — | ✔ |  |
| keyset_depth_10000 | 2.27 ms | — | ✔ |  |
| offset_depth_100000 | 2.31 ms | — | ✔ |  |
| keyset_depth_100000 | 2.36 ms | — | ✔ |  |

### Redis 8 (k3s)

Speed-up: **×1.0** (best: offset_depth_10000)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| offset_depth_10000 | 2.22 ms | — | ✔ |  |
| keyset_depth_10000 | 2.42 ms | — | ✔ |  |
| offset_depth_100000 | 2.33 ms | — | ✔ |  |
| keyset_depth_100000 | 2.60 ms | — | ✔ |  |

### Elasticsearch 9

Speed-up: **×4.82** (best: keyset_depth_100000)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| offset_depth_10000 | 57 ms | — | ✔ |  |
| keyset_depth_10000 | 13 ms | — | ✔ |  |
| offset_depth_100000 | 213 ms | — | ✔ |  |
| keyset_depth_100000 | 12 ms | — | ✔ |  |

### Elasticsearch 9 (k3s)

Speed-up: **×4.14** (best: keyset_depth_10000)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| offset_depth_10000 | 57 ms | — | ✔ |  |
| keyset_depth_10000 | 14 ms | — | ✔ |  |
| offset_depth_100000 | 205 ms | — | ✔ |  |
| keyset_depth_100000 | 14 ms | — | ✔ |  |

### Cassandra 5.0

Speed-up: **×31.62** (best: keyset_depth_10000)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| offset_depth_10000 | 91 ms | — | ✔ |  |
| keyset_depth_10000 | 2.88 ms | — | ✔ |  |
| offset_depth_100000 | 662 ms | — | ✔ |  |
| keyset_depth_100000 | 3.37 ms | — | ✔ |  |

### Cassandra 5.0 (k3s)

Speed-up: **×32.83** (best: keyset_depth_10000)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| offset_depth_10000 | 97 ms | — | ✔ |  |
| keyset_depth_10000 | 2.95 ms | — | ✔ |  |
| offset_depth_100000 | 676 ms | — | ✔ |  |
| keyset_depth_100000 | 3.65 ms | — | ✔ |  |

## opt04_batching

**Write batching: 2000 events one-by-one vs 2 bulk requests of 1000**

_round trips dominate small writes; every engine has a bulk path (insert_many, pipeline, batch, _bulk)_

### MongoDB 7.0

Speed-up: **×86.21** (best: bulk_put_1000_x2)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| single_put_x2000 | — | 253 | ✔ |  |
| bulk_put_1000_x2 | — | 21,811 | ✔ |  |

### MongoDB 7.0 (k3s)

Speed-up: **×116.4** (best: bulk_put_1000_x2)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| single_put_x2000 | — | 297 | ✔ |  |
| bulk_put_1000_x2 | — | 34,571 | ✔ |  |

### LMDB

Speed-up: **×2.72** (best: bulk_put_1000_x2)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| single_put_x2000 | — | 16,294 | ✔ |  |
| bulk_put_1000_x2 | — | 44,292 | ✔ |  |

### RocksDB

Speed-up: **×1.83** (best: bulk_put_1000_x2)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| single_put_x2000 | — | 23,403 | ✔ |  |
| bulk_put_1000_x2 | — | 42,866 | ✔ |  |

### Neo4j 5

Speed-up: **×34.58** (best: bulk_put_1000_x2)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| single_put_x2000 | — | 211 | ✔ |  |
| bulk_put_1000_x2 | — | 7,296 | ✔ |  |

### Neo4j 5 (k3s)

Speed-up: **×43.72** (best: bulk_put_1000_x2)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| single_put_x2000 | — | 200 | ✔ |  |
| bulk_put_1000_x2 | — | 8,743 | ✔ |  |

### Neo4j 5 Enterprise cluster

Speed-up: **×53.63** (best: bulk_put_1000_x2)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| single_put_x2000 | — | 102 | ✔ |  |
| bulk_put_1000_x2 | — | 5,470 | ✔ |  |

### Neo4j 5 Enterprise cluster (k3s)

Speed-up: **×34.23** (best: bulk_put_1000_x2)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| single_put_x2000 | — | 141 | ✔ |  |
| bulk_put_1000_x2 | — | 4,827 | ✔ |  |

### Redis 8

Speed-up: **×6.73** (best: bulk_put_1000_x2)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| single_put_x2000 | — | 2,134 | ✔ |  |
| bulk_put_1000_x2 | — | 14,361 | ✔ |  |

### Redis 8 (k3s)

Speed-up: **×13.93** (best: bulk_put_1000_x2)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| single_put_x2000 | — | 1,499 | ✔ |  |
| bulk_put_1000_x2 | — | 20,882 | ✔ |  |

### Elasticsearch 9

Speed-up: **×51.22** (best: bulk_put_1000_x2)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| single_put_x2000 | — | 186 | ✔ |  |
| bulk_put_1000_x2 | — | 9,526 | ✔ |  |

### Elasticsearch 9 (k3s)

Speed-up: **×54.11** (best: bulk_put_1000_x2)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| single_put_x2000 | — | 176 | ✔ |  |
| bulk_put_1000_x2 | — | 9,523 | ✔ |  |

### Cassandra 5.0

Speed-up: **×5.39** (best: bulk_put_1000_x2)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| single_put_x2000 | — | 707 | ✔ |  |
| bulk_put_1000_x2 | — | 3,810 | ✔ |  |

### Cassandra 5.0 (k3s)

Speed-up: **×6.97** (best: bulk_put_1000_x2)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| single_put_x2000 | — | 578 | ✔ |  |
| bulk_put_1000_x2 | — | 4,026 | ✔ |  |

## opt05_multi_get

**100 point reads: sequential round trips vs one multi-get request**

_batched reads (MGET / $in / IN / mget / pipelining) amortise the network round trip_

### MongoDB 7.0

Speed-up: **×38.98** (best: multi_get_100)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| sequential_100_gets | 114 ms | — | ✔ |  |
| multi_get_100 | 2.92 ms | — | ✔ |  |

### MongoDB 7.0 (k3s)

Speed-up: **×54.02** (best: multi_get_100)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| sequential_100_gets | 78 ms | — | ✔ |  |
| multi_get_100 | 1.44 ms | — | ✔ |  |

### LMDB

Speed-up: **×1.09** (best: multi_get_100)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| sequential_100_gets | 0.89 ms | — | ✔ |  |
| multi_get_100 | 0.81 ms | — | ✔ |  |

### RocksDB

Speed-up: **×1.66** (best: multi_get_100)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| sequential_100_gets | 1.84 ms | — | ✔ |  |
| multi_get_100 | 1.10 ms | — | ✔ |  |

### Neo4j 5

Speed-up: **×17.08** (best: multi_get_100)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| sequential_100_gets | 212 ms | — | ✔ |  |
| multi_get_100 | 12 ms | — | ✔ |  |

### Neo4j 5 (k3s)

Speed-up: **×17.71** (best: multi_get_100)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| sequential_100_gets | 206 ms | — | ✔ |  |
| multi_get_100 | 12 ms | — | ✔ |  |

### Neo4j 5 Enterprise cluster

Speed-up: **×28.31** (best: multi_get_100)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| sequential_100_gets | 249 ms | — | ✔ |  |
| multi_get_100 | 8.79 ms | — | ✔ |  |

### Neo4j 5 Enterprise cluster (k3s)

Speed-up: **×23.46** (best: multi_get_100)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| sequential_100_gets | 215 ms | — | ✔ |  |
| multi_get_100 | 9.16 ms | — | ✔ |  |

### Redis 8

Speed-up: **×13.56** (best: multi_get_100)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| sequential_100_gets | 36 ms | — | ✔ |  |
| multi_get_100 | 2.62 ms | — | ✔ |  |

### Redis 8 (k3s)

Speed-up: **×10.35** (best: multi_get_100)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| sequential_100_gets | 32 ms | — | ✔ |  |
| multi_get_100 | 3.12 ms | — | ✔ |  |

### Elasticsearch 9

Speed-up: **×17.36** (best: multi_get_100)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| sequential_100_gets | 291 ms | — | ✔ |  |
| multi_get_100 | 17 ms | — | ✔ |  |

### Elasticsearch 9 (k3s)

Speed-up: **×18.49** (best: multi_get_100)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| sequential_100_gets | 328 ms | — | ✔ |  |
| multi_get_100 | 18 ms | — | ✔ |  |

### Cassandra 5.0

Speed-up: **×7.41** (best: multi_get_100)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| sequential_100_gets | 174 ms | — | ✔ |  |
| multi_get_100 | 24 ms | — | ✔ |  |

### Cassandra 5.0 (k3s)

Speed-up: **×8.46** (best: multi_get_100)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| sequential_100_gets | 220 ms | — | ✔ |  |
| multi_get_100 | 26 ms | — | ✔ |  |

## opt06_embedding_vs_reference

**Order with its items: embedded array (one document) vs referenced (order + items lookup)**

_embedding what is read together avoids a second query / join; referencing keeps documents small and items independently addressable_

### MongoDB 7.0

Speed-up: **×1.0** (best: embedded_items)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| embedded_items | 1.33 ms | — | ✔ |  |
| referenced_items | 2.90 ms | — | ✔ |  |

### MongoDB 7.0 (k3s)

Speed-up: **×1.0** (best: embedded_items)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| embedded_items | 0.62 ms | — | ✔ |  |
| referenced_items | 2.35 ms | — | ✔ |  |

### LMDB

Speed-up: **×1.0** (best: embedded_items)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| embedded_items | 0.01 ms | — | ✔ |  |
| referenced_items | 0.03 ms | — | ✔ |  |

### RocksDB

Speed-up: **×1.0** (best: embedded_items)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| embedded_items | 0.03 ms | — | ✔ |  |
| referenced_items | 0.06 ms | — | ✔ |  |

### Neo4j 5

Speed-up: **×1.01** (best: referenced_items)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| embedded_items | 2.80 ms | — | ✔ |  |
| referenced_items | 2.77 ms | — | ✔ |  |

### Neo4j 5 (k3s)

Speed-up: **×1.11** (best: referenced_items)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| embedded_items | 2.77 ms | — | ✔ |  |
| referenced_items | 2.50 ms | — | ✔ |  |

### Neo4j 5 Enterprise cluster

Speed-up: **×1.11** (best: referenced_items)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| embedded_items | 2.64 ms | — | ✔ |  |
| referenced_items | 2.38 ms | — | ✔ |  |

### Neo4j 5 Enterprise cluster (k3s)

Speed-up: **×1.04** (best: referenced_items)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| embedded_items | 2.08 ms | — | ✔ |  |
| referenced_items | 2.00 ms | — | ✔ |  |

### Redis 8

Speed-up: **×1.0** (best: embedded_items)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| embedded_items | 0.56 ms | — | ✔ |  |
| referenced_items | 1.35 ms | — | ✔ |  |

### Redis 8 (k3s)

Speed-up: **×1.0** (best: embedded_items)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| embedded_items | 0.55 ms | — | ✔ |  |
| referenced_items | 1.15 ms | — | ✔ |  |

### Elasticsearch 9

Speed-up: **×1.0** (best: embedded_items)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| embedded_items | 3.03 ms | — | ✔ |  |
| referenced_items | 8.82 ms | — | ✔ |  |

### Elasticsearch 9 (k3s)

Speed-up: **×1.0** (best: embedded_items)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| embedded_items | 2.88 ms | — | ✔ |  |
| referenced_items | 9.01 ms | — | ✔ |  |

### Cassandra 5.0

Speed-up: **×1.0** (best: embedded_items)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| embedded_items | 1.14 ms | — | ✔ |  |
| referenced_items | 2.19 ms | — | ✔ |  |

### Cassandra 5.0 (k3s)

Speed-up: **×1.0** (best: embedded_items)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| embedded_items | 1.80 ms | — | ✔ |  |
| referenced_items | 4.35 ms | — | ✔ |  |

## opt07_write_durability

**Single-document write at each durability / write-concern level the engine offers**

_acknowledgement level (replica acks, fsync, refresh) is the biggest lever on write latency_

### MongoDB 7.0

Speed-up: **×1.0** (best: w0_unacknowledged)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| w0_unacknowledged | 0.32 ms | — | ✔ |  |
| w1 | 1.46 ms | — | ✔ |  |
| w1_journal | 2.18 ms | — | ✔ |  |
| w_majority | 4.54 ms | — | ✔ |  |
| w_majority_journal | 4.54 ms | — | ✔ |  |

### MongoDB 7.0 (k3s)

Speed-up: **×1.0** (best: w0_unacknowledged)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| w0_unacknowledged | 0.18 ms | — | ✔ |  |
| w1 | 0.70 ms | — | ✔ |  |
| w1_journal | 1.06 ms | — | ✔ |  |
| w_majority | 3.75 ms | — | ✔ |  |
| w_majority_journal | 3.37 ms | — | ✔ |  |

### LMDB

n/a

### RocksDB

n/a

### Neo4j 5

n/a

### Neo4j 5 (k3s)

n/a

### Neo4j 5 Enterprise cluster

n/a

### Neo4j 5 Enterprise cluster (k3s)

n/a

### Redis 8

Speed-up: **×1.0** (best: ack_primary)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| ack_primary | 0.69 ms | — | ✔ |  |
| wait_1_replica | 15 ms | — | ✔ |  |
| waitaof_local_fsync | 1.00 s | — | ✔ |  |
| waitaof_local+1_replica | 1.00 s | — | ✔ |  |

### Redis 8 (k3s)

Speed-up: **×1.0** (best: ack_primary)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| ack_primary | 0.56 ms | — | ✔ |  |
| wait_1_replica | 24 ms | — | ✔ |  |
| waitaof_local_fsync | 1.00 s | — | ✔ |  |
| waitaof_local+1_replica | 1.00 s | — | ✔ |  |

### Elasticsearch 9

Speed-up: **×1.0** (best: refresh_false)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| refresh_false | 5.06 ms | — | ✔ |  |
| refresh_true | 31 ms | — | ✔ |  |
| wait_for | 30.02 s | — | ✔ |  |

### Elasticsearch 9 (k3s)

Speed-up: **×1.0** (best: refresh_false)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| refresh_false | 5.93 ms | — | ✔ |  |
| refresh_true | 32 ms | — | ✔ |  |
| wait_for | 30.02 s | — | ✔ |  |

### Cassandra 5.0

Speed-up: **×1.0** (best: ONE)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| ONE | 0.90 ms | — | ✔ |  |
| LOCAL_QUORUM | 1.35 ms | — | ✔ |  |
| ALL | 1.57 ms | — | ✔ |  |

### Cassandra 5.0 (k3s)

Speed-up: **×1.0** (best: ONE)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| ONE | 1.26 ms | — | ✔ |  |
| LOCAL_QUORUM | 1.93 ms | — | ✔ |  |
| ALL | 2.14 ms | — | ✔ |  |

## opt08_read_consistency

**Point read at each read consistency level / read preference the engine offers**

_linearizable / quorum reads cost extra round trips; replica reads trade freshness for offload_

### MongoDB 7.0

Speed-up: **×1.37** (best: secondaryPreferred@replica)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| primary | 0.68 ms | — | ✔ |  |
| local | 0.65 ms | — | ✔ |  |
| majority | 0.71 ms | — | ✔ |  |
| secondaryPreferred@replica | 0.49 ms | — | ✔ |  |
| linearizable | 3.54 ms | — | ✔ |  |

### MongoDB 7.0 (k3s)

Speed-up: **×1.2** (best: secondaryPreferred@replica)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| primary | 0.54 ms | — | ✔ |  |
| local | 0.49 ms | — | ✔ |  |
| majority | 0.52 ms | — | ✔ |  |
| secondaryPreferred@replica | 0.45 ms | — | ✔ |  |
| linearizable | 3.39 ms | — | ✔ |  |

### LMDB

n/a

### RocksDB

n/a

### Neo4j 5

n/a

### Neo4j 5 (k3s)

n/a

### Neo4j 5 Enterprise cluster

n/a

### Neo4j 5 Enterprise cluster (k3s)

n/a

### Redis 8

Speed-up: **×1.13** (best: replica@replica)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| primary | 0.46 ms | — | ✔ |  |
| replica@replica | 0.41 ms | — | ✔ |  |

### Redis 8 (k3s)

Speed-up: **×1.0** (best: primary)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| primary | 0.49 ms | — | ✔ |  |
| replica@replica | 0.52 ms | — | ✔ |  |

### Elasticsearch 9


| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| default | 3.24 ms | — | ✔ |  |

### Elasticsearch 9 (k3s)


| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| default | 3.27 ms | — | ✔ |  |

### Cassandra 5.0

Speed-up: **×1.0** (best: ONE)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| ONE | 1.22 ms | — | ✔ |  |
| LOCAL_QUORUM | 1.84 ms | — | ✔ |  |
| ALL | 1.55 ms | — | ✔ |  |

### Cassandra 5.0 (k3s)

Speed-up: **×1.02** (best: ALL)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| ONE | 1.65 ms | — | ✔ |  |
| LOCAL_QUORUM | 1.62 ms | — | ✔ |  |
| ALL | 1.61 ms | — | ✔ |  |

## opt09_connection_reuse

**Point read with a new connection per operation vs one persistent client**

_handshakes (TCP, TLS, auth, topology discovery) dwarf a point read; always pool_

### MongoDB 7.0

Speed-up: **×9.79** (best: persistent_client)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| connect_per_operation | 5.10 ms | — | ✔ |  |
| persistent_client | 0.52 ms | — | ✔ |  |

### MongoDB 7.0 (k3s)

Speed-up: **×8.68** (best: persistent_client)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| connect_per_operation | 4.36 ms | — | ✔ |  |
| persistent_client | 0.50 ms | — | ✔ |  |

### LMDB

Speed-up: **×1.11** (best: persistent_client)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| connect_per_operation | 0.01 ms | — | ✔ |  |
| persistent_client | 0.01 ms | — | ✔ |  |

### RocksDB

Speed-up: **×1.23** (best: persistent_client)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| connect_per_operation | 0.02 ms | — | ✔ |  |
| persistent_client | 0.01 ms | — | ✔ |  |

### Neo4j 5

Speed-up: **×3.45** (best: persistent_client)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| connect_per_operation | 8.51 ms | — | ✔ |  |
| persistent_client | 2.46 ms | — | ✔ |  |

### Neo4j 5 (k3s)

Speed-up: **×2.61** (best: persistent_client)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| connect_per_operation | 5.03 ms | — | ✔ |  |
| persistent_client | 1.93 ms | — | ✔ |  |

### Neo4j 5 Enterprise cluster

Speed-up: **×7.68** (best: persistent_client)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| connect_per_operation | 23 ms | — | ✔ |  |
| persistent_client | 2.99 ms | — | ✔ |  |

### Neo4j 5 Enterprise cluster (k3s)

Speed-up: **×6.72** (best: persistent_client)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| connect_per_operation | 13 ms | — | ✔ |  |
| persistent_client | 1.97 ms | — | ✔ |  |

### Redis 8

Speed-up: **×11.48** (best: persistent_client)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| connect_per_operation | 4.29 ms | — | ✔ |  |
| persistent_client | 0.37 ms | — | ✔ |  |

### Redis 8 (k3s)

Speed-up: **×8.34** (best: persistent_client)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| connect_per_operation | 4.70 ms | — | ✔ |  |
| persistent_client | 0.56 ms | — | ✔ |  |

### Elasticsearch 9

Speed-up: **×2.43** (best: persistent_client)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| connect_per_operation | 6.65 ms | — | ✔ |  |
| persistent_client | 2.74 ms | — | ✔ |  |

### Elasticsearch 9 (k3s)

Speed-up: **×2.28** (best: persistent_client)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| connect_per_operation | 7.18 ms | — | ✔ |  |
| persistent_client | 3.15 ms | — | ✔ |  |

### Cassandra 5.0

Speed-up: **×24.38** (best: persistent_client)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| connect_per_operation | 38 ms | — | ✔ |  |
| persistent_client | 1.55 ms | — | ✔ |  |

### Cassandra 5.0 (k3s)

Speed-up: **×26.23** (best: persistent_client)

| variant | p50 ms | rows/s | ok | note |
|---|---:|---:|---|---|
| connect_per_operation | 31 ms | — | ✔ |  |
| persistent_client | 1.18 ms | — | ✔ |  |

