# Logging

Container log driver and rotation (compose x-logging), the engine's own log format and location, then three live
checks: a slow-query log (one point read that must not be logged, one known-slow operation that must, both read
back through the engine's own interface), an audit log round trip, and a log-level change at run time.
Log volume = what the engine wrote to stdout so far.

| engine | container logs | server log | slow-query log | slow op → logged | audit log | run-time level |
|---|---|---|---|---|---|---|
| MongoDB 7.0 | json-file 50m×3 (json, 23.2 MB, 3 containers) | json (stdout (docker logs) — systemLog.path when set; getLog: global = last 1024 lines in memory) | ✔ 100 ms threshold; db.setProfilingLevel(1, {slowms: 100, sampleRate: 1.0}) on 'lab' (system.profile capped 1 MB + 'Slow query' JSON log lines) | 1.10 s → 1.10 s (fast op 0.90 ms not logged) | unsupported | ✔ setParameter logComponentVerbosity.command -1 -> 1 -> -1 (no restart; every command logged at 1) |
| MongoDB 7.0 (k3s) | kubelet (container-log-max-size/-files via k3d.yaml) 50Mi×3 (json, 28.5 MB, 3 containers) | json (stdout (docker logs) — systemLog.path when set; getLog: global = last 1024 lines in memory) | ✔ 100 ms threshold; db.setProfilingLevel(1, {slowms: 100, sampleRate: 1.0}) on 'lab' (system.profile capped 1 MB + 'Slow query' JSON log lines) | 1.30 s → 1.30 s (fast op 1.30 ms not logged) | unsupported | ✔ setParameter logComponentVerbosity.command -1 -> 1 -> -1 (no restart; every command logged at 1) |
| LMDB | embedded: no container | none (in-process library: log in the host application) | unsupported |  | unsupported | no server log level (RocksDB info_log_level is an open-time option; LMDB has no log) |
| RocksDB | embedded: no container | text (RocksDB info LOG in the DB dir) (data/rocksdb/lab.rocks/LOG) | unsupported |  | unsupported | no server log level (RocksDB info_log_level is an open-time option; LMDB has no log) |
| Neo4j 5 | json-file 50m×3 (text, 7 KB, 1 container) | text (log4j2 PatternLayout; JsonTemplateLayout available) (/logs/{neo4j,debug,query,security,http}.log + stdout) | ✔ 100 ms threshold; db.logs.query.threshold=100ms is configured but Community writes nothing to query.log; using the query collector: CALL db.stats.clear/collect('QUERIES') (in-memory, every query with elapsed µs; filter >= 100 ms) | 925 ms → 723 ms (fast op 85 ms not logged) | unsupported | ✔ edited conf/server-logs.xml Root level INFO->DEBUG; log4j2 monitorInterval=30 picked it up after 20.9s (DEBUG lines appeared in debug.log); reverted |
| Neo4j 5 (k3s) | kubelet (container-log-max-size/-files via k3d.yaml) 50Mi×3 (text, 1 KB, 1 container) | text (log4j2 PatternLayout; JsonTemplateLayout available) (/logs/{neo4j,debug,query,security,http}.log + stdout) | ✔ 100 ms threshold; db.logs.query.threshold=100ms is configured but Community writes nothing to query.log; using the query collector: CALL db.stats.clear/collect('QUERIES') (in-memory, every query with elapsed µs; filter >= 100 ms) | 645 ms → 571 ms (fast op 59 ms not logged) | unsupported | ✔ edited conf/server-logs.xml Root level INFO->DEBUG; log4j2 monitorInterval=30 picked it up after 4.7s (DEBUG lines appeared in debug.log); reverted |
| Neo4j 5 Enterprise cluster | json-file 50m×3 (text, 8 KB, 3 containers) | text (log4j2 PatternLayout; JsonTemplateLayout available) (/logs/{neo4j,debug,query,security,http}.log + stdout) | ✔ 100 ms threshold; CALL dbms.setConfigValue('db.logs.query.threshold', '100ms') (dynamic) -> /logs/query.log on the member that ran the query | 361 ms → 359 ms (fast op 31 ms not logged) | ✔ 1 entries; /logs/security.log (Enterprise): authentication and authorization events; provoking a failed login + a denied write | ✔ CALL dbms.setConfigValue('db.logs.query.threshold', '250ms') -> SHOW SETTINGS verified -> back to 100ms (dynamic, no restart); log4j2 monitorInterval reload also available |
| Neo4j 5 Enterprise cluster (k3s) | kubelet (container-log-max-size/-files via k3d.yaml) 50Mi×3 (text, 8 KB, 3 containers) | text (log4j2 PatternLayout; JsonTemplateLayout available) (/logs/{neo4j,debug,query,security,http}.log + stdout) | ✔ 100 ms threshold; CALL dbms.setConfigValue('db.logs.query.threshold', '100ms') (dynamic) -> /logs/query.log on the member that ran the query | 428 ms → 424 ms (fast op 44 ms not logged) | ✔ 3 entries; /logs/security.log (Enterprise): authentication and authorization events; provoking a failed login + a denied write | NotALeader: {neo4j_code: Neo.ClientError.Cluster.NotALeader} {message: Unable to route to database 'lab'. Unable to get bolt address of leader.} {gql_status: 08N00} {gql_status_description: error: con |
| Redis 8 | json-file 50m×3 (text, 95 KB, 6 containers) | text (stdout (docker logs) — `logfile` when set) | ✔ 100 ms threshold; CONFIG SET slowlog-log-slower-than 100000 (µs) + SLOWLOG RESET; latency-monitor-threshold 100 ms | 226 ms → 221 ms (fast op 0.30 ms not logged) | ✔ 2 entries; ACL LOG RESET; ACL SETUSER nslab_auditor on >s3cret ~* +@read (read-only user to provoke denials) | ✔ CONFIG SET loglevel debug -> verified -> back to notice (no restart) |
| Redis 8 (k3s) | kubelet (container-log-max-size/-files via k3d.yaml) 50Mi×3 (text, 52 KB, 6 containers) | text (stdout (docker logs) — `logfile` when set) | ✔ 100 ms threshold; CONFIG SET slowlog-log-slower-than 100000 (µs) + SLOWLOG RESET; latency-monitor-threshold 100 ms | 232 ms → 221 ms (fast op 0.50 ms not logged) | ✔ 2 entries; ACL LOG RESET; ACL SETUSER nslab_auditor on >s3cret ~* +@read (read-only user to provoke denials) | ✔ CONFIG SET loglevel debug -> verified -> back to notice (no restart) |
| Elasticsearch 9 | json-file 50m×3 (json, 347 KB, 3 containers) | json (ECS) (stdout (docker logs); logs/ dir when running from the tarball) | ✔ 100 ms threshold; PUT lab_*/_settings index.search.slowlog.threshold.query.warn=100ms (+fetch, indexing) — dynamic, per index | 555 ms → 466 ms (fast op 22 ms not logged) | unsupported | ✔ PUT _cluster/settings transient logger.org.elasticsearch.snapshots=DEBUG -> verified -> reset (no restart) |
| Elasticsearch 9 (k3s) | kubelet (container-log-max-size/-files via k3d.yaml) 50Mi×3 (json, 376 KB, 3 containers) | json (ECS) (stdout (docker logs); logs/ dir when running from the tarball) | ✔ 100 ms threshold; PUT lab_*/_settings index.search.slowlog.threshold.query.warn=100ms (+fetch, indexing) — dynamic, per index | 667 ms → 562 ms (fast op 3.70 ms not logged) | unsupported | ✔ PUT _cluster/settings transient logger.org.elasticsearch.snapshots=DEBUG -> verified -> reset (no restart) |
| Cassandra 5.0 | json-file 50m×3 (text, 4.3 MB, 3 containers) | text (/opt/cassandra/logs/{system,debug,gc}.log + stdout) | ✔ 500 ms threshold; slow_query_log_timeout=500ms from cassandra.yaml (settings vtable is read-only: InvalidRequest: Error from server: code=2200 [Invalid query]) -> debug.log via MonitoringTask (5 s report interval) | 4.16 s → 1.74 s (fast op 2.40 ms not logged) | ✔ 3 entries; nodetool enableauditlog --included-keyspaces lab (BinAuditLogger -> /opt/cassandra/logs/audit, Chronicle queue) | ✔ nodetool setlogginglevel org.apache.cassandra.db.Keyspace TRACE -> verified -> DEBUG (per node, no restart) |
| Cassandra 5.0 (k3s) | kubelet (container-log-max-size/-files via k3d.yaml) 50Mi×3 (text, 1.6 MB, 3 containers) | text (/opt/cassandra/logs/{system,debug,gc}.log + stdout) | ✔ 500 ms threshold; slow_query_log_timeout=500ms from cassandra.yaml (settings vtable is read-only: InvalidRequest: Error from server: code=2200 [Invalid query]) -> debug.log via MonitoringTask (5 s report interval) | 3.61 s → 1.34 s (fast op 2.70 ms not logged) | ✔ 3 entries; nodetool enableauditlog --included-keyspaces lab (BinAuditLogger -> /opt/cassandra/logs/audit, Chronicle queue) | ✔ nodetool setlogginglevel org.apache.cassandra.db.Keyspace TRACE -> verified -> DEBUG (per node, no restart) |

## Logging strategy notes

### MongoDB 7.0

- **server log**: JSON lines on stdout ({t, s, c, id, ctx, msg, attr}); systemLog.path + logRotate for files; getLog reads the in-memory ring
- **slow query**: operationProfiling: mode slowOp/all, slowOpThresholdMs, slowOpSampleRate -> capped system.profile per database + 'Slow query' log lines (durationMillis, planSummary, keysExamined/docsExamined)
- **audit**: auditLog is Enterprise / Percona PSMDB (destination file|syslog|console, filter) — not in Community
- **runtime**: setParameter logLevel / logComponentVerbosity, db.setProfilingLevel(level, {slowms, sampleRate}) — no restart
- slow-log entry: `command lab.events {"aggregate": "events", "pipeline": [{"$group": {"_id": "$event_type", "n": {"$sum": 1}}}], "cursor": {}}` — 1.10 s
- slow-log entry: `command lab.events {"aggregate": "events", "pipeline": [{"$group": {"_id": "$event_type", "n": {"$sum": 1}}}], "cursor": {}}` — 1.20 s
- slow-log entry: `command lab.events {"createIndexes": "events", "indexes": [{"key": {"customer_id": 1}, "ns": "lab.events", "name": "customer_id_1"}], "ignoreUnknownIndexOptions` — 3.35 s
- slow-log entry: `command lab.order_items {"createIndexes": "order_items", "indexes": [{"key": {"order_id": 1}, "name": "order_id_1", "ns": "lab.order_items"}, {"key": {"product_` — 2.68 s

### MongoDB 7.0 (k3s)

- **server log**: JSON lines on stdout ({t, s, c, id, ctx, msg, attr}); systemLog.path + logRotate for files; getLog reads the in-memory ring
- **slow query**: operationProfiling: mode slowOp/all, slowOpThresholdMs, slowOpSampleRate -> capped system.profile per database + 'Slow query' log lines (durationMillis, planSummary, keysExamined/docsExamined)
- **audit**: auditLog is Enterprise / Percona PSMDB (destination file|syslog|console, filter) — not in Community
- **runtime**: setParameter logLevel / logComponentVerbosity, db.setProfilingLevel(level, {slowms, sampleRate}) — no restart
- slow-log entry: `command lab.events {"aggregate": "events", "pipeline": [{"$group": {"_id": "$event_type", "n": {"$sum": 1}}}], "cursor": {}}` — 1.30 s
- slow-log entry: `command lab.order_items {"createIndexes": "order_items", "indexes": [{"key": {"order_id": 1}, "name": "order_id_1", "ns": "lab.order_items"}, {"key": {"product_` — 4.08 s
- slow-log entry: `command lab.products {"createIndexes": "products", "indexes": [{"key": {"attrs.tags": 1}, "name": "attrs.tags_1", "ns": "lab.products"}, {"key": {"_fts": "text"` — 3.17 s
- slow-log entry: `command lab.events {"createIndexes": "events", "indexes": [{"key": {"customer_id": 1}, "name": "customer_id_1", "ns": "lab.events"}], "ignoreUnknownIndexOptions` — 3.45 s

### LMDB

- **server log**: no server process: the host application logs (Python logging / structlog) — the harness writes results/logs/nslab-*.jsonl
- **slow query**: instrument the call site (timing decorator, OpenTelemetry span per transaction)
- **audit**: application-level
- **runtime**: application-level

### RocksDB

- **server log**: no server process: the host application logs (Python logging / structlog) — the harness writes results/logs/nslab-*.jsonl
- **slow query**: instrument the call site (timing decorator, OpenTelemetry span per transaction)
- **audit**: application-level
- **runtime**: application-level

### Neo4j 5

- **server log**: /logs/neo4j.log (user log, also stdout), /logs/debug.log (root logger, 20 MB x 7 rolling), query.log / security.log / http.log — log4j2 via conf/server-logs.xml + user-logs.xml
- **slow query**: db.logs.query.enabled + db.logs.query.threshold write query.log with parameters/page hits — Enterprise only in practice (accepted but empty in Community); Community: CALL db.stats.collect/retrieve('QUERIES') in-memory query collector
- **audit**: security.log (authentication/authorization events) is Enterprise; no statement audit in Community
- **runtime**: log4j2 monitorInterval=30: edit conf/server-logs.xml and the level changes within 30 s, no restart; dbms.setConfigValue is Enterprise
- slow-log entry: `MATCH (e:Event) WHERE e.value_num > 0.5 RETURN count(e)` — 723 ms
- slow-log entry: `[/logs/query.log] 0 bytes (db.logs.query.enabled=INFO, threshold 100ms — Enterprise feature, empty in Community)` — —

### Neo4j 5 (k3s)

- **server log**: /logs/neo4j.log (user log, also stdout), /logs/debug.log (root logger, 20 MB x 7 rolling), query.log / security.log / http.log — log4j2 via conf/server-logs.xml + user-logs.xml
- **slow query**: db.logs.query.enabled + db.logs.query.threshold write query.log with parameters/page hits — Enterprise only in practice (accepted but empty in Community); Community: CALL db.stats.collect/retrieve('QUERIES') in-memory query collector
- **audit**: security.log (authentication/authorization events) is Enterprise; no statement audit in Community
- **runtime**: log4j2 monitorInterval=30: edit conf/server-logs.xml and the level changes within 30 s, no restart; dbms.setConfigValue is Enterprise
- slow-log entry: `MATCH (e:Event) WHERE e.value_num > 0.5 RETURN count(e)` — 571 ms
- slow-log entry: `[/logs/query.log] 0 bytes (db.logs.query.enabled=INFO, threshold 100ms — Enterprise feature, empty in Community)` — —

### Neo4j 5 Enterprise cluster

- **server log**: /logs/neo4j.log (user log, also stdout), /logs/debug.log (root logger, 20 MB x 7 rolling), query.log / security.log / http.log — log4j2 via conf/server-logs.xml + user-logs.xml
- **slow query**: db.logs.query.enabled=INFO + db.logs.query.threshold -> /logs/query.log on every member (Enterprise: works; parameters, page hits, planning time per query)
- **audit**: /logs/security.log (authentication / authorization events, Enterprise)
- **runtime**: CALL dbms.setConfigValue for dynamic settings (db.logs.query.threshold ...) — no restart
- slow-log entry: `2026-09-14 10:44:26.471+0000 INFO  transaction id:69 - 359 ms: (planning: 34, waiting: 0) - 472 B - 15634 page hits, 69 page faults - bolt-session	bolt	neo4j-py` — 359 ms

### Neo4j 5 Enterprise cluster (k3s)

- **server log**: /logs/neo4j.log (user log, also stdout), /logs/debug.log (root logger, 20 MB x 7 rolling), query.log / security.log / http.log — log4j2 via conf/server-logs.xml + user-logs.xml
- **slow query**: db.logs.query.enabled=INFO + db.logs.query.threshold -> /logs/query.log on every member (Enterprise: works; parameters, page hits, planning time per query)
- **audit**: /logs/security.log (authentication / authorization events, Enterprise)
- **runtime**: CALL dbms.setConfigValue for dynamic settings (db.logs.query.threshold ...) — no restart
- slow-log entry: `2026-09-14 12:35:34.086+0000 INFO  transaction id:69 - 424 ms: (planning: 58, waiting: 0) - 472 B - 15703 page hits, 0 page faults - bolt-session	bolt	neo4j-pyt` — 424 ms
- slow-log entry: `2026-09-14 12:35:34.086+0000 INFO  transaction id:69 - 424 ms: (planning: 58, waiting: 0) - 472 B - 15703 page hits, 0 page faults - bolt-session	bolt	neo4j-pyt` — 424 ms

### Redis 8

- **server log**: plain text on stdout (`pid:role date level message`), loglevel notice; the docker json-file driver rotates it
- **slow query**: SLOWLOG (in-memory ring: slowlog-log-slower-than µs, slowlog-max-len) + LATENCY MONITOR (latency-monitor-threshold ms) + LATENCY DOCTOR
- **audit**: ACL LOG for denied AUTH / NOPERM attempts (in memory, acllog-max-len); MONITOR is a live debug stream, not a log; persistent audit is Redis Enterprise
- **runtime**: CONFIG SET loglevel / slowlog-* / latency-monitor-threshold apply immediately (CONFIG REWRITE persists them)
- slow-log entry: `KEYS customers:*` — 221 ms
- slow-log entry: `LATENCY LATEST event=command latest=220 ms max=220 ms` — 220 ms

### Redis 8 (k3s)

- **server log**: plain text on stdout (`pid:role date level message`), loglevel notice; the docker json-file driver rotates it
- **slow query**: SLOWLOG (in-memory ring: slowlog-log-slower-than µs, slowlog-max-len) + LATENCY MONITOR (latency-monitor-threshold ms) + LATENCY DOCTOR
- **audit**: ACL LOG for denied AUTH / NOPERM attempts (in memory, acllog-max-len); MONITOR is a live debug stream, not a log; persistent audit is Redis Enterprise
- **runtime**: CONFIG SET loglevel / slowlog-* / latency-monitor-threshold apply immediately (CONFIG REWRITE persists them)
- slow-log entry: `KEYS customers:*` — 221 ms
- slow-log entry: `LATENCY LATEST event=command latest=220 ms max=220 ms` — 220 ms

### Elasticsearch 9

- **server log**: ECS JSON on stdout (docker image log4j2): event.dataset elasticsearch.server / deprecation / index_search_slowlog / index_indexing_slowlog
- **slow query**: per-index slowlog thresholds (index.search.slowlog.threshold.{query,fetch}.{warn,info,debug,trace}, index.indexing.slowlog.threshold.index.*), dynamic; lines carry took_millis, source, total_hits
- **audit**: xpack.security.audit.enabled (JSON audit of authentication/authorization/run_as) — needs security on; off in this lab
- **runtime**: PUT _cluster/settings {transient|persistent: {logger.<package>: DEBUG}} — no restart
- slow-log entry: `index.search.slowlog.query [lab_events][0] {\"size\":1,\"query\":{\"script_score\":{\"query\":{\"match_all\":{\"boost\":1.0}},\"script\":{\"source\":\"(doc['val` — 466 ms

### Elasticsearch 9 (k3s)

- **server log**: ECS JSON on stdout (docker image log4j2): event.dataset elasticsearch.server / deprecation / index_search_slowlog / index_indexing_slowlog
- **slow query**: per-index slowlog thresholds (index.search.slowlog.threshold.{query,fetch}.{warn,info,debug,trace}, index.indexing.slowlog.threshold.index.*), dynamic; lines carry took_millis, source, total_hits
- **audit**: xpack.security.audit.enabled (JSON audit of authentication/authorization/run_as) — needs security on; off in this lab
- **runtime**: PUT _cluster/settings {transient|persistent: {logger.<package>: DEBUG}} — no restart
- slow-log entry: `index.search.slowlog.query [lab_events][0] {\"size\":1,\"query\":{\"script_score\":{\"query\":{\"match_all\":{\"boost\":1.0}},\"script\":{\"source\":\"(doc['val` — 562 ms

### Cassandra 5.0

- **server log**: logback text: system.log (INFO), debug.log (DEBUG, rolling 20 MB) and gc.log under /opt/cassandra/logs, mirrored to stdout
- **slow query**: slow_query_log_timeout (500 ms, cassandra.yaml — not mutable at run time in 5.0.x): MonitoringTask aggregates slow reads every 5 s into debug.log ('<query>, time N msec - slow timeout 500 msec')
- **audit**: nodetool enableauditlog (BinAuditLogger under logs/audit, read with auditlogviewer; FileAuditLogger via logback) + full query log (enablefullquerylog / fqltool)
- **runtime**: nodetool setlogginglevel <logger> <level> per node (logback), getlogginglevels; audit/FQL/incremental backups toggled with nodetool too
- slow-log entry: `SELECT * FROM lab.orders ALLOW FILTERING` — 1.74 s

### Cassandra 5.0 (k3s)

- **server log**: logback text: system.log (INFO), debug.log (DEBUG, rolling 20 MB) and gc.log under /opt/cassandra/logs, mirrored to stdout
- **slow query**: slow_query_log_timeout (500 ms, cassandra.yaml — not mutable at run time in 5.0.x): MonitoringTask aggregates slow reads every 5 s into debug.log ('<query>, time N msec - slow timeout 500 msec')
- **audit**: nodetool enableauditlog (BinAuditLogger under logs/audit, read with auditlogviewer; FileAuditLogger via logback) + full query log (enablefullquerylog / fqltool)
- **runtime**: nodetool setlogginglevel <logger> <level> per node (logback), getlogginglevels; audit/FQL/incremental backups toggled with nodetool too
- slow-log entry: `SELECT * FROM lab.orders ALLOW FILTERING` — 1.34 s

