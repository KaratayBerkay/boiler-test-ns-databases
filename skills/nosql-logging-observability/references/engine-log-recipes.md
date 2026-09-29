# Logging recipes per engine (verified in the ns-databases lab, Sept 2026)

What `harness/nslab/engines/<adapter>.py` `slow_query_*` / `audit_log_*` / `log_level_runtime` / `server_log_facts`
run, plus the compose-level settings the stacks ship with.

## Container level (every stack)
```yaml
x-logging: &logging            # compose.yaml, referenced as `logging: *logging` on every service
  driver: json-file
  options: {max-size: "50m", max-file: "3", compress: "true"}
```
`docker logs <c>` keeps working; `docker inspect <c> --format '{{.LogPath}}'` is the file a shipper tails
(promtail/fluent-bit/otel-collector `filelog` on `/var/lib/docker/containers/*/*-json.log`). `local` driver = same
rotation, compressed, faster, but not readable by file-tailing shippers; `journald`/`loki` drivers ship directly.
Measured volume over the lab phases: MongoDB ~4 MB/member (JSON, chatty at start-up), Cassandra 1-2 MB/node,
Elasticsearch 240 KB, Redis 50 KB, Neo4j 7 KB (its logs go to /logs files, only the user log reaches stdout).

## Redis 8
```bash
redis-server ... --slowlog-log-slower-than 10000 --slowlog-max-len 1024 --latency-monitor-threshold 100 --loglevel notice
redis-cli CONFIG SET slowlog-log-slower-than 100000        # µs, immediate; CONFIG REWRITE persists
redis-cli SLOWLOG RESET; redis-cli KEYS 'customers:*' >/dev/null
redis-cli SLOWLOG GET 10        # id, unix ts, µs, argv, client addr, client name  -> KEYS customers:* 212 ms
redis-cli LATENCY LATEST; redis-cli LATENCY DOCTOR         # event=command latest/max ms since latency-monitor-threshold
redis-cli ACL LOG 10            # denied AUTH / NOPERM attempts: reason=auth|command, object, username, count (in memory)
redis-cli CONFIG SET loglevel debug                        # server log = stdout text "pid:role date level message"
```
No persistent statement audit in OSS (MONITOR is a live stream; Redis Enterprise/Software have audit logs).

## MongoDB 7
```bash
mongod ... --profile 1 --slowms 100 --slowOpSampleRate 1.0   # operationProfiling: mode slowOp, per-database system.profile (1 MB capped)
mongosh lab --eval 'db.setProfilingLevel(1, {slowms: 100, sampleRate: 1.0})'           # run time, per database
mongosh lab --eval 'db.system.profile.find({millis:{$gte:100}}).sort({ts:-1}).limit(5)' # op, ns, millis, planSummary, docsExamined
mongosh --eval 'db.adminCommand({getLog:"global"}).log.filter(l => l.includes("Slow query")).slice(-3)'   # the JSON log ring (1024 lines), no file access needed
mongosh --eval 'db.adminCommand({setParameter:1, logComponentVerbosity:{command:{verbosity:1}}})'          # every command logged; 0/-1 to revert
```
JSON log line: `{"t":{"$date":...},"s":"I","c":"COMMAND","id":51803,"ctx":"conn12","msg":"Slow query","attr":{"ns":"lab.events","durationMillis":1202,"planSummary":"COLLSCAN",...}}`.
Audit log (`auditLog.destination file|syslog|console` + `filter`) is Enterprise / Percona Server for MongoDB — see the
vendored `percona-lab__skills__psmdb-features`.

## Cassandra 5.0
```bash
# slow reads: slow_query_log_timeout (500 ms default, cassandra.yaml; UPDATE system_views.settings is rejected in 5.0.x)
#   -> MonitoringTask logs every 5 s (monitoring_report_interval) into debug.log:
#   "<SELECT * FROM lab.orders ALLOW FILTERING>, time 1736 msec - slow timeout 500 msec"   (the internal read command, not the CQL text)
docker exec nslab-cassandra1 grep 'slow timeout' /opt/cassandra/logs/debug.log | tail
# audit log (statements, per node, binary Chronicle queue)
nodetool enableauditlog --included-keyspaces lab             # BinAuditLogger -> /opt/cassandra/logs/audit/*.cq4
nodetool getauditlog; nodetool disableauditlog
/opt/cassandra/tools/bin/auditlogviewer /opt/cassandra/logs/audit | grep ^LogMessage
#   user:anonymous|host:/10.99.1.11:7000|source:/10.99.1.1|port:41706|timestamp:...|type:UPDATE|category:DML|ks:lab|scope:scratch|operation:INSERT INTO scratch ...
# full query log (every query incl. bind values, replayable): nodetool enablefullquerylog --path /var/lib/cassandra/fql ; fqltool dump <path>
nodetool setlogginglevel org.apache.cassandra.db.Keyspace TRACE; nodetool getlogginglevels     # logback, per node, no restart
```
Logs: `/opt/cassandra/logs/system.log` (INFO), `debug.log` (DEBUG, 20 MB rolling), `gc.log`; mirrored to stdout.
`FileAuditLogger` writes through logback (`audit.log`) if you prefer text over the binary queue.

## Neo4j 5 Community
```bash
# query.log: db.logs.query.enabled=INFO + db.logs.query.threshold=100ms are ACCEPTED (SHOW SETTINGS shows them, isDynamic)
# but the file stays EMPTY in Community even for a 3 s query -> query logging is effectively Enterprise.
# Community alternative: the in-memory query collector (on by default)
cypher-shell "CALL db.stats.clear('QUERIES'); CALL db.stats.collect('QUERIES')"
cypher-shell "CALL db.stats.retrieve('QUERIES') YIELD data RETURN data.query, data.invocationSummary.executionTimeInUs.max"
#   -> MATCH (e:Event) WHERE e.value_num > 0.5 RETURN count(e)  722 800 µs  (plus compile time, plan, invocation timestamps)
# run-time log level: dbms.setConfigValue is Enterprise; log4j2 re-reads conf/server-logs.xml every 30 s
docker exec -u neo4j nslab-neo4j sed -i 's/<Root level="INFO">/<Root level="DEBUG">/' /var/lib/neo4j/conf/server-logs.xml   # DEBUG lines in /logs/debug.log after ~21 s
```
Files under `/logs`: `neo4j.log` (user log, also stdout), `debug.log` (root logger, 20 MB × 7), `query.log`,
`security.log` (Enterprise auth events), `http.log`. JSON: switch the `PatternLayout` to `JsonTemplateLayout` in
`server-logs.xml`/`user-logs.xml`.

## Elasticsearch 9
```bash
# per-index slowlog thresholds (dynamic): set them in an index template so new indices inherit them
curl -XPUT 'localhost:9200/lab_*/_settings' -H 'Content-Type: application/json' -d '{
  "index.search.slowlog.threshold.query.warn":"100ms","index.search.slowlog.threshold.fetch.warn":"100ms",
  "index.indexing.slowlog.threshold.index.warn":"100ms"}'
docker logs --since 5m nslab-es | grep slowlog | jq -r '[."elasticsearch.slowlog.took_millis", ."log.logger", ."elasticsearch.slowlog.source"] | @tsv'
#   202  index.search.slowlog.query  {"size":1,"query":{"script_score":...}}      (ECS JSON, event.dataset elasticsearch.index_search_slowlog)
curl -XPUT localhost:9200/_cluster/settings -H 'Content-Type: application/json' -d '{"transient":{"logger.org.elasticsearch.snapshots":"DEBUG"}}'   # null to reset
```
Server log = ECS JSON on stdout in the docker image (`event.dataset`: `elasticsearch.server`, `.deprecation`,
`.index_search_slowlog`, `.index_indexing_slowlog`). Audit (`xpack.security.audit.enabled`) needs security enabled.

## LMDB / RocksDB (embedded)
No server, no slow log: instrument the call site (a timing decorator around transactions, an OpenTelemetry span per
operation) and log from the application. RocksDB keeps an info `LOG` in the DB directory — the lab opens it with
`set_max_log_file_size(50 MB)`, `set_keep_log_file_num(3)`, `set_stats_dump_period_sec(60)` so compaction/flush
events and `rocksdb.stats` dumps are rotated; `db.property_value("rocksdb.stats")` reads the counters live.

## The harness's own log (application-side pattern)
`results/logs/nslab.jsonl` — every console line plus `phase_start` / `phase_end` / `phase_failed` events with
`run_id`, `stack`, `phase`, `seconds`; stdlib `RotatingFileHandler` 20 MB × 5; `NSLAB_LOG_LEVEL`, `NSLAB_LOG_FILE`.
One JSON object per line is what Loki/ELK/OTel ingest without a parser.
