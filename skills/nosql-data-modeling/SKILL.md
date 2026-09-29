---
name: nosql-data-modeling
description: Model data for NoSQL engines by starting from the queries, not the entities - one design per family (document, key-value, wide-column, graph, search, vector, embedded). Covers embedding vs referencing, denormalisation and one-table-per-query, partition/clustering keys, application-maintained index keyspaces, and which access patterns each family cannot express natively (measured across Redis, MongoDB, Cassandra, Neo4j, Elasticsearch, LMDB, RocksDB). Use when choosing a NoSQL store, designing collections/tables/keys, migrating a relational schema, or deciding what to embed, denormalise or push to a second system.
---

# NoSQL data modeling

## The one rule: design from the access patterns, then pick the family
Relational modeling normalises entities and writes queries later. NoSQL inverts this: **list every read and write the
application will do, then choose the family and the key/index layout that serves them.** The lab's operation catalog
(28 operations against one e-commerce dataset) shows what each family can and cannot express — the "n/a" cells are the
design constraints.

| access pattern | best family | why (measured) |
|---|---|---|
| get/put by key, counters, TTL, cache, queues | key-value (Redis) / embedded (LMDB, RocksDB) | 0.02–0.3 ms point ops; no query engine to pay for |
| whole entities read/written together, flexible fields, secondary + geo + vector on one store | document (MongoDB) | point 0.8 ms, `$in`/`$lookup`/`$graphLookup`/2dsphere all native |
| massive write throughput, known partition-scoped queries, multi-DC | wide-column (Cassandra) | 1.25 ms writes, leaderless RF3; but one table per query and no ad-hoc scans |
| connected data, variable-depth traversal, path/relationship queries | graph (Neo4j) | traversal & join 1.8–2.0 ms native; joins ARE the storage |
| full-text, faceted search, analytics/aggregation over any field | search (Elasticsearch) | aggregation 7 ms, text 7 ms; but ~3 ms point reads and no transactions |
| approximate nearest neighbour over embeddings | vector (Redis/Neo4j/Cassandra/ES HNSW; dedicated Qdrant/Milvus) | kNN 0.8–9.5 ms with an HNSW index vs ~55 ms brute-force |

## Embedding vs referencing (the document decision)
- **Embed what you read together and write together** (order + its line items). The lab's `opt06` shows embedded-items
  reads beat referenced ~2× on Redis and Cassandra and are equal on MongoDB — but embedding an unbounded, independently
  updated child (events per customer) grows the document without bound. Rule: embed 1:few and read-mostly; reference
  1:many and write-heavy or independently queried.
- **Wide-column**: embedding is a JSON/serialized column; there are no joins, so you **denormalise into one table per
  query** and accept multiple copies. Cassandra's `orders_by_customer`, `orders_by_time`, `products_by_category` are the
  same rows keyed three ways. Getting the partition key right is the whole game: it must appear in every query, distribute
  evenly, and bound partition size (bucket time-series by day/month).
- **Key-value / embedded**: you build the "indexes" yourself as extra keyspaces — a sorted set / ordered key range per
  access path (`idx:orders:by_time`, `i/e_type/<type>/<ts>/<id>`). This is exactly what the lab does; it is why a missing
  index costs ×6 000–13 000 (full scan) there.

## What no NoSQL family gives you for free
- **Server-side GROUP BY over a non-key field**: only search engines (ES aggregations) and MongoDB/Neo4j pipelines. KV,
  wide-column and embedded stores scan client-side (1.7–4.9 s on this dataset). If you need ad-hoc analytics, model a
  rollup, stream to a search/OLAP system, or keep the data relational.
- **Multi-document ACID with rollback**: MongoDB replica-set transactions and Neo4j transactions yes; Cassandra only a
  single-partition conditional (LWT) batch; Redis `MULTI/EXEC` is atomic-but-no-rollback; Elasticsearch none.
- **Arbitrary joins**: only graph (native) and document (`$lookup`). Everywhere else: denormalise or join in the app.

## Migrating a relational schema
1. Enumerate the queries the app runs against the SQL schema (slow log / ORM call sites).
2. Group by entity read together → candidate documents/aggregates. Keep foreign keys that are traversed as references
   or embeds; drop ones only used for ad-hoc joins you can denormalise.
3. For each remaining access path with no natural key, decide: secondary index (document/search), query table
   (wide-column), index keyspace (KV/embedded), or relationship (graph).
4. Re-check the "n/a" list above — if a required pattern is n/a for your chosen family, either change family or plan a
   second system for that pattern (e.g. document store of record + search index for full-text/analytics).

See `references/family-cheatsheet.md` for the per-engine key/index syntax and the exact modelling used in the lab.
