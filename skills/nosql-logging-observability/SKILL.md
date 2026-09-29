---
name: nosql-logging-observability
description: Logging strategy for NoSQL engines in containers - docker json-file rotation, JSON/ECS structured server logs, slow-query logs (Redis SLOWLOG/LATENCY, MongoDB profiler + system.profile + getLog, Cassandra slow_query_log_timeout/debug.log, Neo4j query.log vs the db.stats query collector, Elasticsearch index slowlogs), audit logs (Redis ACL LOG, Cassandra audit/full-query log, Enterprise-only audit in MongoDB/Neo4j/ES), changing log levels at run time, log volume per engine, and shipping to Loki/ELK/OTel. Verified round trips with measured numbers. Use when asked about logging, slow queries, profiling, audit trails, log rotation, structured logs, observability or "why is nothing in the log".
---

# NoSQL logging & observability

Built from the lab's **logging phase**, which for every stack (1) reads the container log driver, rotation and how much
the engine has logged, (2) records the engine's own log format/location, (3) sets a slow-query threshold, runs one
point read (must not be logged) and one known-slow operation (must be), and reads the entries back **through the
engine's own interface**, (4) enables an audit log, runs statements, reads them back, (5) changes the log level at run
time, (6) measures the log volume produced. Results: `results/<engine>/latest.json` → `phases.logging`.

## Three layers, one strategy
1. **Container**: every stack's compose file carries one `x-logging` anchor — `json-file`, `max-size 50m`, `max-file 3`,
   `compress true` — so a chatty engine cannot fill the host and `docker logs` / a file-tailing shipper keep working.
2. **Engine**: JSON where the engine offers it (MongoDB, Elasticsearch), the slow-query facility switched on with a
   threshold, the audit log available for when it is needed, verbosity changeable without a restart.
3. **Application**: the harness writes `results/logs/nslab.jsonl` (rotated JSON lines with `run_id/stack/phase`,
   phase events with seconds) — the pattern for services that use embedded stores like LMDB/RocksDB, which have no
   server log at all.

## What each engine gives you (verified)

| engine | server log | slow-query log | audit log | run-time level | logged during the lab phases |
|---|---|---|---|---|---|
| Redis 8 | text on stdout | ✅ `SLOWLOG` (µs threshold, in-memory ring) + `LATENCY LATEST/DOCTOR`; `KEYS customers:*` 222 ms logged as 212 ms | ✅ `ACL LOG` (auth failures, NOPERM) — in memory, not persistent | ✅ `CONFIG SET loglevel` | ~50 KB primary |
| MongoDB 7 | **JSON** on stdout, `getLog` ring | ✅ profiler level 1 / `slowms` → `system.profile` + `"Slow query"` log lines; `$group` over 1M docs 1204 ms logged as 1202 ms, `COLLSCAN` | ❌ Enterprise / Percona PSMDB | ✅ `setParameter logComponentVerbosity` | ~4 MB per member (start-up chatter) |
| Cassandra 5 | text: system.log / debug.log / gc.log + stdout | ✅ `slow_query_log_timeout` **500 ms, not changeable at run time** (settings vtable read-only); MonitoringTask writes the *internal read command* every 5 s: `COUNT(*) orders` 4.2 s client-side → `<SELECT * FROM lab.orders ALLOW FILTERING>, time 1736 msec` | ✅ `nodetool enableauditlog` → binary queue → `auditlogviewer` (user, source, type, category, ks, scope, operation); full query log too | ✅ `nodetool setlogginglevel` per node | 1-2 MB per node |
| Neo4j 5 CE | text files under /logs (log4j2); only neo4j.log on stdout | ⚠️ `db.logs.query.*` accepted but **query.log stays empty in Community** (3 s query not logged); the in-memory **query collector** `db.stats.collect/retrieve('QUERIES')` records every query with elapsed µs (label scan 925 ms → 723 ms exec) | ❌ security.log / query.log are Enterprise | ✅ log4j2 `monitorInterval=30`: edit `server-logs.xml`, DEBUG lines after ~21 s (`dbms.setConfigValue` is Enterprise) | 7 KB stdout |
| Neo4j 5 EE cluster | same files, per member | ✅ `db.logs.query.threshold` set at run time with `dbms.setConfigValue` → `/logs/query.log` **on the member that ran the query** (361 ms → 359 ms line, parameters included) | ✅ `security.log`: failed logins / denied writes | ✅ `dbms.setConfigValue` for dynamic settings | ~2.5 KB stdout per member |
| Elasticsearch 9 | **ECS JSON** on stdout | ✅ per-index `index.search.slowlog.threshold.*` (dynamic); `script_score` over 1M docs 225 ms → `took_millis 202` line with the query source | ❌ needs `xpack.security` (off here) | ✅ `PUT _cluster/settings logger.<pkg>` | 240 KB |
| LMDB / RocksDB | none / RocksDB info `LOG` (rotated 50 MB×3, stats every 60 s) | ❌ instrument the application | ❌ | ❌ open-time options | — |

## Rules that fall out of the numbers
- **Set the slow threshold at the p99 you promise, not at "1 s".** Every engine here lets you pick it (100 ms in the lab)
  except Cassandra, whose 500 ms lives in `cassandra.yaml` and needs a restart — decide it before go-live.
- **Read the slow log through the engine, not by grepping stdout**, so the same check works in a managed service:
  `SLOWLOG GET`, `system.profile` / `getLog`, `db.stats.retrieve`, ES slowlog lines (those *are* stdout — ship them).
  Cassandra is the exception: debug.log on the node (or the full query log via `fqltool`).
- **The logged text is not always your statement.** Cassandra logs the coordinator's read command
  (`SELECT * FROM lab.orders ALLOW FILTERING` for a `COUNT(*)`), MongoDB logs the command document with `planSummary`,
  Redis logs argv. Match on the operation, not the literal string.
- **Audit ≠ slow log.** Only Cassandra (`enableauditlog`, `enablefullquerylog`) and Redis (`ACL LOG`, denials only) do it
  in the open-source edition; MongoDB, Neo4j and Elasticsearch put statement/auth auditing behind Enterprise/security.
  Plan the compliance story before choosing the edition.
- **JSON out of the box only from MongoDB and Elasticsearch.** Redis and Cassandra are text (Cassandra needs a logback
  JSON encoder), Neo4j needs a `JsonTemplateLayout` in `server-logs.xml`. A shipper with a parser per engine is normal.
- **Rotation is not optional.** MongoDB wrote ~4 MB per member just starting up and running the lab; a busy cluster at
  `logLevel 1` fills disks. `max-size`/`max-file` at the container level is the floor; engine-side rotation
  (`logRotate`, logback/log4j2 rolling policies, Neo4j 20 MB × 7) is the second layer.
- **Run-time level changes exist everywhere except embedded stores** — raise verbosity for an incident, then put it back
  (the phase verifies the change and reverts it).

## Shipping
`docker inspect <c> --format '{{.LogPath}}'` → promtail / fluent-bit / OTel collector `filelog` on
`/var/lib/docker/containers/*/*-json.log` with `docker` parsing; add a JSON parser for MongoDB/ES lines and the
`event.dataset` (ES) / `c` component (MongoDB) as labels. For Cassandra/Neo4j file logs mount `/opt/cassandra/logs`
and `/logs` (the lab's `neo4j-logs` volume) into the shipper or use the `journald`/`loki` log driver.

## Related
- `references/engine-log-recipes.md` — exact commands and the log line formats seen.
- `scripts/slowlog.py <stack> [--exercise]` — read an engine's slow log through its own interface.
- Vendored: `skills/vendor/redis__agent-skills__redis-observability` (metrics to alert on, SLOWLOG/INFO/MEMORY DOCTOR),
  `skills/vendor/percona-lab__skills__pmm-operations` (Query Analytics: MongoDB profiler as the QAN source),
  `skills/vendor/percona-lab__skills__psmdb-features` (audit logging and log redaction on PSMDB).
- Sibling skills: `nosql-benchmark-timing` (engine profilers for *measuring*, not logging), `docker-compose-nosql`.
