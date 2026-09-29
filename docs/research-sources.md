# Sources consulted (Sept 2026)

## Images & documentation
- Redis 8: https://hub.docker.com/_/redis, https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/, https://redis.io/docs/latest/develop/interact/search-and-query/ (RediSearch/JSON/vector built into OSS 8)
- MongoDB: https://hub.docker.com/_/mongo, https://www.mongodb.com/docs/manual/replication/, https://www.mongodb.com/docs/manual/core/transactions/; kernel incompatibility SERVER-121912 / SERVER-125742 (jira.mongodb.org)
- Cassandra 5: https://hub.docker.com/_/cassandra, SAI overview https://cassandra.apache.org/doc/latest/cassandra/developing/cql/indexing/sai/, LWT https://cassandra.apache.org/doc/latest/cassandra/developing/cql/dml.html
- Neo4j 5: https://hub.docker.com/_/neo4j, indexes https://neo4j.com/docs/cypher-manual/current/indexes/, vector indexes https://neo4j.com/docs/cypher-manual/current/indexes/semantic-indexes/vector-indexes/
- Elasticsearch 9: https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html, kNN https://www.elastic.co/guide/en/elasticsearch/reference/current/knn-search.html, search_after / aggregations reference
- LMDB: http://www.lmdb.tech/doc/, https://lmdb.readthedocs.io/  ·  RocksDB: https://rocksdb.org/, https://congyuwang.github.io/RocksDict/

## Python drivers
redis-py, pymongo, cassandra-driver (+ libev), neo4j, httpx (Elasticsearch REST), lmdb, rocksdict. Registered for future
adapters (pyproject `exotic` extra): couchbase, aerospike, pymilvus, weaviate-client, qdrant-client, surrealdb, python-arango, etcd3, pymemcache, tarantool, falkordb.

## Agent-skill sources surveyed (for skills/vendor/)
Vendor-authored NoSQL skill repos found on GitHub (Sept 2026): scylladb/agent-skills (Apache-2.0), aerospike/agent-skills
(Apache-2.0), surrealdb/agent-skills (MIT), valkey-io/valkey-skills (BSD-3), opensearch-project/opensearch-agent-skills
(Apache-2.0), memgraph/skills, qdrant/skills (Apache-2.0), weaviate/agent-skills, chroma-core/agent-skills (MIT),
couchbaselabs/agent-skills, neo4j-contrib/neo4j-skills (MIT), redis/* and redis-developer/* (per-repo), mongodb-developer/edd-skill
(Apache-2.0), FalkorDB/skills (MIT). Not vendored: aggregator/mirror repos of unclear provenance (majiayu000/*, clawic/*,
TerminalSkills/*, bytesagain/*), and repos without a clear permissive license.

## Backup & logging pass (2026-09-14)
- Engine docs consulted: Redis persistence (RDB/AOF multi-part manifest, `BGSAVE`, `SLOWLOG`, `LATENCY`, `ACL LOG`),
  MongoDB `mongodump --oplog` / `mongorestore --oplogReplay`, `fsyncLock`, database profiler & `getLog`,
  `logComponentVerbosity`; Cassandra 5 backups (nodetool snapshot `--ttl`, incremental_backups, `nodetool import`,
  sstableloader), audit logging / full query logging (`auditlogviewer`, `fqltool`), `slow_query_log_timeout`,
  `nodetool setlogginglevel`, `system_views.settings`; Neo4j 5 operations manual (`neo4j-admin database dump/load`,
  backup = Enterprise, query logging, log4j2 `server-logs.xml`, `db.stats.*` query collector); Elasticsearch 9 snapshot
  and restore, SLM, index slowlog, `logger.*` cluster settings, `action.destructive_requires_name`; LMDB `mdb_copy` /
  `env.copy`, RocksDB Checkpoint / BackupEngine / info LOG options; Docker `json-file` logging driver options.
- Agent-skill sources searched on GitHub (code search `filename:SKILL.md` + `org:` filters for redis, mongodb,
  mongodb-developer, mongodb-js, elastic, grafana, open-telemetry, scylladb, neo4j-contrib, valkey-io, aerospike,
  couchbaselabs, datastax, percona, Percona-Lab, docker, apache, restic, vmware-tanzu): vendored `redis/agent-skills`
  (redis-observability, MIT), `Percona-Lab/skills` (pbm-recipes, psmdb-features, pmm-operations, MIT),
  `neo4j-contrib/neo4j-skills` (neo4j-cli-tools-skill, MIT). Not vendored: `couchbaselabs/agent-skills` backup skill (no
  license), `elastic/agent-skills` (Apache-2.0; cluster-health/reindex/ESQL skills are not about backup or logging),
  `scylladb/scylla-operator` must-gather skill (Kubernetes-operator specific), aggregator registries.
- Replicas pass (2026-09-14): Elasticsearch multi-node discovery (`discovery.seed_hosts`, `cluster.initial_master_nodes`,
  shared `path.repo`), Neo4j 5 clustering (`server.cluster.system_database_mode`, `dbms.cluster.discovery.endpoints`,
  `initial.dbms.default_primaries_count`, advertised addresses, `neo4j://` routing), Neo4j online backup (`server.backup.*`,
  `neo4j-admin database backup`), seed-from-URI (`dbms.databases.seed_from_uri_providers`, `FileSeedProvider`), Neo4j
  Enterprise evaluation licence (`NEO4J_ACCEPT_LICENSE_AGREEMENT=eval`).
