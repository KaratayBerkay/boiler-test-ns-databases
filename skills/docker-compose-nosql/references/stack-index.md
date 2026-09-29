# nslab stack index (ports and start command)

Start any stack: `uv run --project harness nslab up <key>` · run everything for it: `nslab run <key> --failover`.

| key | engine | topology | host ports |
|---|---|---|---|
| redis | Redis 8 | primary + replica + 3 Sentinels + HAProxy read LB | 16379 (primary), 16380 (replica), 16381 (haproxy read), 26379-26381 (sentinels) |
| mongodb | MongoDB 7 | 3-member replica set rs0 (init container) | 27117 / 27118 / 27119 |
| cassandra | Cassandra 5 | 3 nodes RF=3, static IPs 10.99.1.11-13 | 9042 via container IPs (no host mapping) |
| neo4j | Neo4j 5 Community | single instance + APOC (no replication possible in Community) | 7687 (bolt), 7474 (browser) |
| neo4j-cluster | Neo4j 5 Enterprise (evaluation licence) | 3 primaries in one Raft cluster, dataset in database `lab`, online backup port 6362, file seed provider | 17687 / 17688 / 17689 (bolt, advertised for routing), 17474 (browser) |
| elasticsearch | Elasticsearch 9 | 3-node cluster (all master+data), every index 1 shard + 2 replicas, security off | 19200 / 19201 / 19202 |
| lmdb | LMDB | embedded (in-process) | none — `data/lmdb/lab.mdb` |
| rocksdb | RocksDB | embedded (in-process) | none — `data/rocksdb/lab.rocks` |

Each `stacks/<key>/` holds `compose.yaml` (except embedded) + `lab.yaml` (targets, replication, features). Copy the
closest stack to add a new engine; register its driver adapter in `harness/nslab/engines/__init__.py`.

Adapters already registered for more engines (write `engines/<mod>.py` + `stacks/<key>/` to enable): Valkey/Dragonfly/
KeyDB/Garnet and cluster mode (resp), ScyllaDB/YugabyteDB-YCQL (cql `flavor`), FerretDB/DocumentDB and Memgraph and
OpenSearch (reuse pymongo/bolt/es), plus couchbase, aerospike, qdrant, weaviate, milvus, chroma, surreal, arango, etcd,
memcached, tarantool, ravendb, falkordb, meili, typesense, influx.
