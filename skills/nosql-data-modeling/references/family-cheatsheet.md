# Per-family modelling cheatsheet (as used in the lab)

## Redis (key-value, RESP) — hashes + application-maintained indexes
- Document = hash: `HSET customers:1 name ... email ...` (nested `attrs.lang` flattened to a field). Collection membership: `SADD keys:customers customers:1`.
- Secondary index = set/zset you maintain on write: `SADD idx:orders:cust:42 <oid>`; ordered index = zset scored by `epoch*1e6+id` (`idx:orders:by_time`) → `ZRANGEBYSCORE` for range + keyset pages.
- Inventory composite key uses a **hash tag** so both warehouses of a product share a cluster slot: `inventory:{<pid>}:<w>` (lets `MULTI/EXEC` span them).
- Text + vector: `FT.CREATE ... SCHEMA description TEXT embedding VECTOR HNSW 6 TYPE FLOAT32 DIM 8 DISTANCE_METRIC COSINE` (RediSearch, built into Redis 8). Geo: `GEOADD geo:customers lon lat member` + `GEOSEARCH`.

## MongoDB (document) — store the aggregate, index the access paths
- `_id` = your id; embed `items` in the order; keep a flat `order_items` collection too if items are queried independently.
- Indexes follow the **ESR rule** (Equality, Sort, Range): `createIndex({status:1, ordered_at:1})`, `{category_id:1, price:-1}`, `{location:'2dsphere'}`, `{description:'text'}`. Vector (`$vectorSearch`) is Atlas/mongot-only — not in Community.
- Joins: `$lookup`; trees: `$graphLookup`; grouping: aggregation pipeline (`$group`,`$dateToString`).

## Cassandra (wide-column, CQL) — one table per query, partition key in every WHERE
- `PRIMARY KEY ((partition_key), clustering_col1, clustering_col2)`: partition key chooses the node, clustering columns the sort order within it.
- Denormalise: `orders`, `orders_by_customer ((customer_id), id)`, `orders_by_time ((bucket), ordered_at, id)` are the same rows keyed for different queries.
- Counters need their own table (`qty counter`); compare-and-set is `UPDATE ... IF version = ?` (LWT); atomic multi-row within one partition is a conditional `BATCH`.
- Bind `UNSET_VALUE` for absent columns (a bound `NULL` is a tombstone). SAI (5.0) indexes vectors + numerics; analyzed text needs 5.1.

## Neo4j (graph, Cypher) — relationships are the storage
- `(:Customer)-[:PLACED]->(:Order)-[:CONTAINS {qty,unit_price}]->(:Product)-[:IN_CATEGORY]->(:Category)-[:CHILD_OF]->(:Category)`. Order items are edges → "referenced items" is native and there is no join to pay for.
- Constraints/indexes: `CREATE CONSTRAINT ... REQUIRE n.id IS UNIQUE`, `CREATE INDEX ... FOR (n:L) ON (n.a, n.b)`, `CREATE FULLTEXT INDEX`, `CREATE VECTOR INDEX ... {vector.dimensions, vector.similarity_function}`, `CREATE POINT INDEX`.
- Variable-depth traversal: `MATCH path=(c)-[:CHILD_OF*0..]->(root)`. Load in batches with `UNWIND $rows AS r MERGE ...`.

## Elasticsearch (search, REST) — every field indexed at mapping time
- One index per collection; map `keyword` (exact/agg), `text` (BM25), `date`, `geo_point`, `dense_vector` (HNSW). Objects you never query set `enabled:false`.
- Deep pages: `search_after` with a sort tiebreaker, not `from/size` (capped at `index.max_result_window`, default 10 000).
- No joins across indices, no multi-doc transactions, no per-doc TTL (use an `expires_at` field + ILM/delete-by-query).

## LMDB / RocksDB (embedded) — ordered key-value, you build everything
- `d/<coll>/<id>` = JSON value; index keyspaces `i/<name>/<sortable>/<id>` scanned by prefix/range. Sortable timestamps as zero-padded epoch strings.
- Atomicity via one write transaction (LMDB) / `WriteBatch` (RocksDB). One process opens the file once; share the handle across threads (the GIL caps concurrency anyway).
