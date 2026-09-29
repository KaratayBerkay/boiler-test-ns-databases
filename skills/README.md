# Skills

Two groups of Agent Skills (the open `SKILL.md` format: YAML frontmatter with `name` + `description`, optional `references/` and `scripts/`).
Install into Claude Code by copying or symlinking a skill directory into `~/.claude/skills/` (user-wide) or `.claude/skills/` (project).

## First-party skills (written from this lab's verified results)
| skill | use it for | extras |
|---|---|---|
| `nosql-data-modeling` | query-first modelling per family; embed vs reference; what each family can't express | `references/family-cheatsheet.md` |
| `nosql-operation-mapping` | translate one of 28 data-access operations into each family's native primitive (or n/a) | full read/write mapping tables |
| `nosql-indexing-optimization` | the measured wins: missing-index-is-a-scan, keyset, batching, multi-get, pooling, consistency-as-latency | speed-up numbers per engine |
| `nosql-benchmark-timing` | measuring NoSQL honestly; refresh/consistency traps; multi-process load; real tools per engine | `references/methodology.md`, `scripts/bench_op.py` |
| `nosql-connection-consistency` | pools, limits/storms, write concern / consistency level, CAS & optimistic concurrency, transactions, retries | measured CAS-contention results |
| `nosql-replication-failover` | async/replica-set/leaderless/single-primary topologies, lag, read routing, measured failover downtime | `scripts/lag_probe.py` |
| `docker-compose-nosql` | verified images/env/healthchecks/ulimits, one-shot cluster init, registry mirror | `references/stack-index.md` |
| `nosql-backup-restore` | backup strategy per engine (RDB/AOF, mongodump --oplog, nodetool snapshot + import, neo4j-admin dump, ES snapshots + SLM, LMDB copy, RocksDB checkpoint), measured backup/restore times, artifact sizes, downtime, the restore drill, the traps | `references/engine-recipes.md`, `scripts/backup_drill.py` |
| `nosql-on-kubernetes` | k3d + Harbor + compose→kustomize generation for NoSQL stacks, the `NSLAB_PLATFORM=k3s` harness shim, CronJob backups, what NoSQL engines need on k8s (static IPs, advertised addresses, memlock, JMX, shared RWO PVCs) | `references/generator-notes.md` |
| `nosql-logging-observability` | container log rotation, JSON vs text server logs, slow-query logs (SLOWLOG, profiler, debug.log, query collector, index slowlog), audit logs, run-time log levels, log volume, shipping | `references/engine-log-recipes.md`, `scripts/slowlog.py` |

## Vendored third-party skills (`vendor/`)
Each directory carries a `VENDORED.txt` (source URL + license + date) and the upstream `LICENSE` where the repo ships one.
Only permissive licenses (Apache-2.0 / MIT / BSD-3-Clause). Skills whose upstream repo had no clear license were **not** vendored.

| directory | engine / topic | license | why |
|---|---|---|---|
| `scylladb__agent-skills__data-modeling` | wide-column data modeling (query-first, partitions, anti-patterns) | Apache-2.0 | vendor-authored; the canonical CQL modelling guide |
| `scylladb__agent-skills__vector-search` | vector search on ScyllaDB | Apache-2.0 | vendor-authored |
| `aerospike__agent-skills__data-modeling` | key-value/record data modeling | Apache-2.0 | vendor-authored |
| `valkey-io__valkey-skills__valkey` | Redis/Valkey app patterns (caching, locks, queues, streams, cluster) | BSD-3-Clause | vendor-authored, deep router over references |
| `surrealdb__agent-skills__surrealql` / `__performance` | SurrealQL query writing + performance | MIT | vendor-authored multi-model |
| `neo4j-contrib__neo4j-skills__cypher` / `__modeling` / `__query-tuning` | Cypher, graph modeling, query tuning | MIT | Neo4j-contrib authored; covers the graph family (incl. Memgraph-compatible Cypher) |
| `qdrant__skills__performance-optimization` | vector DB indexing/memory/search-speed tuning | Apache-2.0 | vendor-authored; the dedicated-vector family |
| `chroma-core__agent-skills__chroma-local` | Chroma (embedded vector) usage | MIT | vendor-authored |
| `opensearch-project__agent-skills__search` | OpenSearch/Elasticsearch search skill | Apache-2.0 | vendor-authored; the search family |
| `mongodb-developer__edd-skill` | MongoDB Entity-Document-Diagram schema notation | Apache-2.0 (declared in repo README) | MongoDB-developer authored document modelling |
| `redis__agent-skills__redis-observability` | Redis metrics to watch, SLOWLOG / INFO / MEMORY DOCTOR / FT.PROFILE triage, Prometheus wiring | MIT | Redis Inc.'s official skill repo; the logging/observability side of the key-value family |
| `percona-lab__skills__pbm-recipes` | Percona Backup for MongoDB: logical / physical / incremental backups, PITR, selective restore, storage config | MIT | Percona-authored; the production backup tool the lab's `mongodump` strategy grows into |
| `percona-lab__skills__psmdb-features` | Percona Server for MongoDB: hot backup, audit logging, log redaction, encryption (Enterprise features, open source) | MIT | Percona-authored; answers "audit log on MongoDB without Enterprise" |
| `percona-lab__skills__pmm-operations` | Percona Monitoring & Management: Query Analytics (profiler as the QAN source), custom metrics, alerting | MIT | Percona-authored; the observability stack around the slow-query logs |
| `neo4j-contrib__neo4j-skills__cli-tools` | `neo4j-admin` dump / load / backup / restore / copy, cypher-shell, Aura CLI reference | MIT | Neo4j-contrib authored; the backup/restore side of the graph family |

Surveyed and **not** vendored: aggregator/mirror repos of unclear provenance (`majiayu000/claude-skill-registry`,
`clawic/skills`, `TerminalSkills/skills`, `bytesagain/ai-skills`, `G1Joshi/Agent-Skills`), `memgraph/skills` (no
license file in the repo), `couchbaselabs/agent-skills` `skills/backup` (cbbackupmgr — no license file or declaration
in the repo) and `elastic/agent-skills` `elasticsearch-cluster-health` (Apache-2.0, but a read-only triage flow bound to
the `elastic` CLI, not a backup/logging skill). See `../docs/research-sources.md` for the full survey.

Gaps the first-party skills fill: no existing skill covered *cross-family* NoSQL modelling, the operation→primitive
mapping (with n/a), honest cross-engine NoSQL benchmarking, compose/replication/failover measurement, a cross-engine
backup/restore drill with measured RPO/RTO numbers, or a cross-engine logging strategy (slow-query/audit/run-time
level round trips) in SKILL.md form.
