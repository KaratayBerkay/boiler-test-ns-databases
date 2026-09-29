---
name: nosql-operation-mapping
description: Translate a data-access operation (point read, batch read, partial update, atomic increment, compare-and-set, secondary/range/nested/array query, top-N, offset vs keyset pagination, group-by and time-bucket aggregation, join, graph traversal, full-text, geo radius, vector kNN, multi-document transaction, TTL) into the native primitive of each NoSQL family - or recognise that it has no native expression there. Use when writing code against Redis/Valkey, MongoDB/FerretDB, Cassandra/ScyllaDB, Neo4j/Memgraph, Elasticsearch/OpenSearch, or an embedded KV store, or when deciding whether one store covers all the app's operations.
---

# NoSQL operation mapping

Each row is one operation from the lab's catalog and how each family expresses it. `n/a` = no native way (do it in
application code or a second system). Latencies (p50, one 28-core host, scale-1) show the cost, not just feasibility.

## Reads
| operation | Redis (KV) | MongoDB (doc) | Cassandra (wide-col) | Neo4j (graph) | Elasticsearch (search) | embedded (LMDB/RocksDB) |
|---|---|---|---|---|---|---|
| point read by key | `HGETALL` 0.2ms | `find_one(_id)` 0.8ms | `SELECT ... WHERE id=?` 1.8ms | `MATCH (n{id}) ` 2.7ms | `GET /_doc/id` 2.8ms | `get(key)` 0.02ms |
| batch read (100) | pipeline/`MGET` | `find({_id:{$in}})` | 100 concurrent reads (not `IN`) | `WHERE id IN $ids` | `_mget` | 100 gets |
| secondary lookup | index set you maintain | index on field | query table `((cust),id)` | relationship match | any field indexed | index keyspace scan |
| range on time | zset `ZRANGEBYSCORE` | compound index range | clustering-col range | index range | `range` filter | ordered key range |
| nested-field filter | flattened field | dot-notation (native) | flatten to columns | property match | object/nested field | client scan |
| array-contains | member set | array field (native) | `set<>` + `CONTAINS` | `x IN list` | multi-valued field | client scan |
| top-N sorted | zset reverse range | index + sort + limit | clustering order + limit | index + `ORDER BY` | sort + size | reverse index range |
| offset pagination | zset by rank | `skip()` | **n/a** (page & discard) | `SKIP` | `from/size` (≤10k) | count & discard |
| keyset pagination | zset score cursor | `$gt` on sort key | `(a,b)>(?,?)` tuple | `WHERE > cursor` | `search_after` | key range from cursor |
| group-by aggregate | **client scan** | pipeline `$group` | **client scan** | `WITH ... count()` | aggregations (native) | **client scan** |
| join (2 collections) | **client/Lua** | `$lookup` | **n/a** (denormalise) | traversal (native) | **n/a** across indices | **client** |
| graph traversal | client loop | `$graphLookup` | **n/a** | `[:R*1..n]` (native) | **n/a** | client loop |
| full-text | RediSearch `FT.SEARCH` | `$text` | SAI text (5.1+) / scan | fulltext index | BM25 (native) | scan |
| geo radius | `GEOSEARCH` | `$nearSphere` (2dsphere) | scan + haversine | `point.distance` | `geo_distance` | scan + haversine |
| vector kNN | `FT.SEARCH KNN` | `$vectorSearch` (Atlas only) | SAI `ANN OF` | `db.index.vector` | `knn` (native) | brute force |

## Writes
| operation | Redis | MongoDB | Cassandra | Neo4j | Elasticsearch | embedded |
|---|---|---|---|---|---|---|
| insert / upsert | `HSET`/`SET` | `insert`/`replace upsert` | `INSERT` (upsert) | `MERGE` | `PUT _doc` | `put` |
| partial field update | `HSET one field` | `$set` | `UPDATE SET col` | `SET n.p` | `_update` (reindex) | read-modify-write |
| atomic increment | `HINCRBY` | `$inc findOneAndUpdate` | counter column | guarded `SET` in tx | painless `_update` | RMW under lock |
| compare-and-set | Lua / `WATCH` / `SET IFEQ` | filter on version | LWT `IF version=?` | guarded `SET` | `if_seq_no`+`if_primary_term` | RMW |
| bulk write (1000) | pipeline | `insert_many` | `execute_concurrent`/`BATCH` | `UNWIND` | `_bulk` | write batch |
| multi-doc transaction | `MULTI/EXEC` (no rollback) | replica-set txn (ACID) | single-partition `BATCH` only | tx (ACID) | **n/a** | write batch (atomic) |
| write with TTL | `SET EX` / `HEXPIRE` | TTL index | `USING TTL` | **n/a** (APOC/sweep) | **n/a** (expiry field) | **n/a** (RocksDB TTL CF) |

## How to use
1. Write down the operations your feature needs from the two tables.
2. If any cell for your candidate engine is **n/a**, decide: change engine, denormalise/precompute, or add a second
   system for that operation (common: document store of record + search index for text/analytics/aggregation).
3. Prefer the native primitive over emulating it — the emulated path is where the big latencies live (see
   `nosql-indexing-optimization`: a missing index is a full scan, ×6 000–13 000 slower on KV/embedded stores).
