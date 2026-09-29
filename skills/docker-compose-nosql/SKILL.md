---
name: docker-compose-nosql
description: Author Docker Compose stacks for NoSQL databases that come up reliably - verified images and tags (Sept 2026) for Redis/Valkey/Dragonfly, MongoDB, Cassandra/ScyllaDB, Neo4j, Elasticsearch/OpenSearch and embedded stores, with correct bootstrap env vars, healthchecks, one-shot init containers for cluster wiring (replica-set initiate, Sentinel, Cassandra seed join), volumes, memory/ulimit settings, and how to pull images without hitting Docker Hub's rate limit (mirror.gcr.io). Use when writing or debugging compose files for NoSQL clusters, replica sets, Sentinels, or search/graph engines.
---

# Docker Compose for NoSQL databases

## Patterns that make NoSQL stacks reliable
1. **Every service gets a real healthcheck** and dependents wait on it; one-shot init containers use `restart: "no"` and
   dependents wait with `condition: service_completed_successfully`. `nslab up` waits for healthy/exited-0 and prints logs
   on failure.
2. **Wire clusters from a one-shot init container**, not entrypoint hacks: MongoDB `rs.initiate()`, (Cassandra joins via
   `CASSANDRA_SEEDS` + sequential `depends_on: service_healthy`, no init needed), Redis Sentinel from a config file.
   Make it idempotent (check `rs.status()` first) so re-running is safe.
3. **Pin the tag you tested** and record the resolved version in `lab.yaml`/notes (`redis:8`=8.10.1, `mongo:7`=7.0.37,
   `cassandra:5`=5.0.9, `neo4j:5-community`=5.26.30, `elasticsearch:9.5.3`).
4. **Named volume per node**, `down -v` between runs for a clean baseline.
5. **Set memory + ulimits explicitly** — defaults are tiny or will refuse to start: Elasticsearch `ES_JAVA_OPTS -Xms/-Xmx`
   + `memlock -1` + `nofile 65536` + `bootstrap.memory_lock`; Cassandra `MAX_HEAP_SIZE`/`HEAP_NEWSIZE` + `memlock -1`;
   Neo4j `server.memory.heap.*` + `server.memory.pagecache.size`; MongoDB `--wiredTigerCacheSizeGB` + `nofile`; Redis
   `--maxclients` + `--maxmemory` (+ `--appendonly`).
6. **Avoid Docker Hub's anonymous limit** (100 pulls / 6 h): pull via `mirror.gcr.io/library/<img>` and retag
   (`scripts/pull-images.sh`). Elasticsearch is on `docker.elastic.co`, not Hub.
7. **Escape `$` as `$$`** in compose `command:` scripts (compose interpolates `$VAR`).

## Verified images and bootstrap (Sept 2026)
| engine | image | key env / command | ports | healthcheck |
|---|---|---|---|---|
| Redis 8 | `redis:8` | `--appendonly yes --appendfsync everysec --replicaof <primary> 6379 --replica-read-only yes --maxclients 10000` | 6379 | `redis-cli ping` / `info replication \| grep master_link_status:up` |
| Redis Sentinel | `redis:8` | `redis-sentinel /conf/sentinel.conf` with `sentinel monitor mymaster <primary> 6379 2`, `down-after 2000`, `resolve-hostnames yes` | 26379 | `redis-cli -p 26379 sentinel master mymaster` |
| MongoDB 7 | `mongo:7` | `mongod --replSet rs0 --bind_ip_all --wiredTigerCacheSizeGB 4 --maxConns 5000`; one-shot `mongosh --file rs-init.js` | 27017 | `mongosh --eval "db.adminCommand('ping').ok"` |
| Cassandra 5 | `cassandra:5` | `CASSANDRA_CLUSTER_NAME`, `CASSANDRA_SEEDS=<node1-ip>`, `MAX_HEAP_SIZE=4G`, `CASSANDRA_NUM_TOKENS=16`; static IPs, join sequentially | 9042 | `nodetool status \| grep ^UN && cqlsh -e 'describe keyspaces'` |
| Neo4j 5 | `neo4j:5-community` | `NEO4J_AUTH=neo4j/<pw>`, `NEO4J_PLUGINS='["apoc"]'`, `NEO4J_server_memory_heap_max__size=4G`, `NEO4J_server_memory_pagecache_size=2G` | 7687 / 7474 | `cypher-shell -u neo4j -p <pw> 'RETURN 1'` |
| Neo4j 5 Enterprise cluster | `neo4j:5-enterprise` + `NEO4J_ACCEPT_LICENSE_AGREEMENT=eval` | per member: `server.cluster.system_database_mode=PRIMARY`, `initial.server.mode_constraint=PRIMARY`, `dbms.cluster.discovery.endpoints=n1:5000,n2:5000,n3:5000`, `initial.dbms.default_primaries_count=3`, `server.{discovery,cluster,cluster.raft}.advertised_address=<member>:{5000,6000,7000}`, `server.bolt.advertised_address=<host-reachable>:<port>` (routing table!), `server.backup.enabled=true` + `listen_address=0.0.0.0:6362`, `dbms.databases.seed_from_uri_providers=FileSeedProvider,...` for file: seeds | 7687 per member | `cypher-shell -d system 'SHOW DATABASE system YIELD currentStatus' \| grep online` — forms in ~45 s |
| Elasticsearch 9 | `elasticsearch:9.5.3` (docker.elastic.co) | single node: `discovery.type=single-node`; cluster: `cluster.name`, `node.name`, `discovery.seed_hosts=es1,es2,es3`, `cluster.initial_master_nodes=es1,es2,es3`; always `xpack.security.enabled=false` (lab), `ES_JAVA_OPTS=-Xms3g -Xmx3g`, `bootstrap.memory_lock=true`, memlock/nofile ulimits, `path.repo=/backups` on a volume shared by every node | 9200 per node | `curl -fs :9200/_cluster/health \| grep -E 'green\|yellow'` |
| Valkey / Dragonfly / KeyDB | `valkey/valkey`, `dragonflydb/dragonfly`, `eqalpha/keydb` | RESP-compatible; same layout as Redis (Dragonfly ignores some flags) | 6379 | `redis-cli ping` |
| ScyllaDB | `scylladb/scylla` | `--seeds`, `--smp`, `--memory`; CQL v4 | 9042 | `nodetool status` |
| OpenSearch | `opensearchproject/opensearch` | `discovery.type=single-node`, `DISABLE_SECURITY_PLUGIN=true`, `OPENSEARCH_JAVA_OPTS` | 9200 | `curl :9200/_cluster/health` |

## Traps found (Sept 2026)
- **MongoDB 8 will not start on Linux kernel 6.19+/7.0** (SERVER-121912, TCMalloc rseq) and the glibc-tunable workaround
  fails; pin `mongo:7` or use MongoDB ≥ 8.0.30/8.3.9-patched, or a kernel < 6.19.
- **Cassandra** needs `start_period` and sequential joins (each node `depends_on` the previous being healthy) or the ring
  splits; give `memlock: -1`. First node up takes ~60–75 s.
- **Elasticsearch** exits instantly without `memlock`/`nofile` ulimits and enough heap; disable the disk watermark on a
  full dev host (`cluster.routing.allocation.disk.threshold_enabled=false`).
- **Neo4j Community** has no clustering/CDC/online-backup — those are Enterprise; don't design a Community stack around them.
  The Enterprise image needs `NEO4J_ACCEPT_LICENSE_AGREEMENT=eval` (evaluation terms) and a 3-primary cluster forms in ~45 s;
  advertise the **host-mapped** Bolt ports (`server.bolt.advertised_address`) or a `neo4j://` client on the host gets
  routing-table addresses it cannot reach. `CREATE DATABASE ... seedURI:'file://...'` fails with "No seed providers found"
  until `FileSeedProvider` is added to `dbms.databases.seed_from_uri_providers` (default is S3/cloud only).
- **Elasticsearch cluster**: an `fs` snapshot repository must be the *same shared path* on every node (one volume mounted
  on all three here); `number_of_replicas=2` on 3 nodes triples write work (bulk load 217k → 112k docs/s) but every node
  can serve every read and losing a node loses nothing.

## Files
- `references/stack-index.md` — what each lab stack contains, its ports, and how to start it.
