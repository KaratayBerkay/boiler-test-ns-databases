# Backup & logging strategies (measured 2026-09-14)

Two phases were added to the harness — `backup` and `logging` — so that every engine's backup story and logging story
are *exercised*, not described. Raw data: `results/<engine>/latest.json` → `phases.backup` / `phases.logging`;
cross-engine tables: `results/SUMMARY.md`; interactive: `docs/report.html`. Scale 1 (1.7M documents), one 28-core host.

## 1. Backup & restore

### What the phase does
1. Fingerprint: counts of `customers/categories/products/inventory`, presence of customers 1-50, a `repl_probe`
   collection seeded with 100 docs.
2. Take **one full backup with the engine's own tooling** while a thread does point reads on the primary (p50 during
   vs. a 2 s baseline before).
3. Mutate: 500 more probe docs, delete customers 1-50.
4. Restore the backup with an availability poller running (100 ms pings; longest failed window = downtime).
5. Verify on a fresh connection: counts match, customers back, probe count back to 100, post-backup writes gone.
6. Where the engine has one, take an incremental second backup after 10 more writes and record its size.

### Results

| engine | backup (method) | online | backup | artifact vs live data | point-read p50 during / baseline | restore (method) | restore | downtime | verified | incremental |
|---|---|---|---|---|---|---|---|---|---|---|
| Redis 8 | `BGSAVE` + copy `dump.rdb` (fork 22 ms, CoW 4 MB) | yes | 5.8 s | 336 MB vs 434 MB `/data` (AOF); 590 MB in memory | 0.59 / 0.54 ms | stop → RDB installed as multi-part AOF base → start → replica full-resync 11.9 s | 24.0 s | **11.4 s** | ✅ | — (RDB is full; AOF is the continuous log) |
| MongoDB 7 | `mongodump --oplog --gzip --archive` (10 collections) | yes | 13.6 s | 51 MB vs 188 MB (storage + index; 267 MB dataSize) | 0.95 / 1.01 ms | `dropDatabase` + `mongorestore --oplogReplay --drop` (1.73M docs + index builds) | 59.5 s | 0 | ✅ | — (PBM does physical/incremental/PITR) |
| MongoDB 7, physical | `fsyncLock` + `cp -a /data/db` + `fsyncUnlock` | writes blocked | 2.1 s blocked | 1.09 GB (incl. oplog + journal) | — | (not exercised: restore = replace the volume on every member) | — | — | — | — |
| Cassandra 5.0 | `nodetool flush` + `snapshot --ttl 3h` (2.1-2.4 s/node, hard links) + export (0.24 s/node) | yes | 8.6 s (3 nodes) | 380 MB = 3 × 126 MB node exports vs 379 MB live SSTables | 1.42 / 1.63 ms | `TRUNCATE` 15 tables (0.8 s) + 45 × `nodetool import --copy-data`, 3 nodes in parallel (~20 s each) | 21.4 s | 0 | ✅ | `enablebackup`: 10.5 KB for 10 rows, 3.2 s incl. flush |
| Neo4j 5 Community | stop → `neo4j-admin database dump` → start | **no** | 33.8 s (**33.6 s down**) | 306 MB dump vs 1.84 GB store + tx logs | 3.6 / 4.2 ms, 581 read errors while stopped | stop → `database load --overwrite-destination` → start | 42.2 s | **42.1 s** | ✅ | — (Enterprise) |
| Elasticsearch 9 (single node run) | `PUT _snapshot/nslab/<tag>` (fs repo, compress; 99 files, 8 indices) + SLM policy | yes | 4.5 s | 166 MB vs 166 MB store (segments) | 2.53 / 2.50 ms | `DELETE` 8 indices by name + `_restore` (wait_for_completion) | 4.4 s | 0 | ✅ | 8.6 KB in 0.09 s (segment-level) |
| Elasticsearch 9, 3-node cluster (current stack) | same, repository on a volume shared by all nodes | yes | 7.2 s | 246 MB (primaries only; after the load test added data) | — | same | 6.9 s | 0 | ✅ | segment-level |
| Neo4j 5 Enterprise cluster | `neo4j-admin database backup lab --from=neo4j1:6362 --type=full` (helper container on the cluster network) | **yes** | 19.8 s | 92 MB vs 1.71 GB store | 2.13 / 2.70 ms | `DROP DATABASE lab` + `CREATE DATABASE lab OPTIONS {existingData:'use', seedURI:'file:///backups/…/lab-….backup'} WAIT` (all 3 primaries seed from the shared file) | 15.9 s | **15.8 s** (the database, not the cluster) | ✅ | differential backups (Enterprise; not exercised) |
| LMDB | `env.copy(compact=True)` inside a read txn | yes | 3.4 s | 829 MB vs 829 MB used pages (8 GB sparse map) | 0.07 / 0.06 ms | close env → copy file → reopen | 6.8 s | 6.8 s (process-local) | ✅ | — |
| RocksDB | `Checkpoint.create_checkpoint` (hard links) after flush | yes | 0.03 s | 171 MB vs 339 MB dir (WAL + LOG included) | 0.06 / 0.09 ms | close → replace dir → reopen | 0.1 s | 0.1 s | ✅ | — (BackupEngine, C++ only) |

### What the numbers say
- **Online backups are free for readers.** Fork + copy-on-write (Redis), hard links (Cassandra, RocksDB), immutable
  segments (Elasticsearch) and MVCC read transactions (LMDB, mongodump's cursor) all left point-read p50 within noise.
  The engine that cannot do it online (Neo4j Community) pays 34 s of downtime per backup.
- **Restore cost depends on what the artifact is.** Storage-format artifacts restore in seconds (ES 4.4 s, Cassandra
  21 s incl. 45 JVM start-ups, RocksDB 0.1 s). Logical dumps have to be re-ingested and re-indexed (MongoDB 60 s for
  1.7M docs). Engines that only load their file at start-up take the service down for the load (Redis 11 s to load a
  336 MB RDB + resync the replica, Neo4j 42 s).
- **Artifact size is a compression story.** mongodump gzip 51 MB for 188 MB on disk (267 MB logical); the Neo4j dump is
  17 % of the store; Redis' RDB is 57 % of used memory; Cassandra and ES artifacts are the (already compressed) files.
- **Incremental exists where the storage is immutable files**: Elasticsearch (segment dedup, 8.6 KB for 10 docs) and
  Cassandra (`incremental_backups` hard-links each flushed SSTable). MongoDB/Redis/Neo4j Community/LMDB are full copies.
- **PITR** needs a shipped change log: MongoDB oplog (PBM/Ops Manager), Cassandra commitlog archiving, Neo4j
  Enterprise transaction logs. Redis AOF and RocksDB WAL replay to the crash point only; ES's RPO is its SLM schedule.

### Disaster-recovery drill — does the backup survive `docker compose down -v`?
The in-place restore above proves the *artifact*; it does not prove you can get the data back after the stack is gone.
The stack's `backups` volume is removed by `down -v` together with the data volumes, so a backup that only lives
there is worthless. `nslab dr <stack>` runs the whole story: fingerprint → backup → **export the artifact to the host**
(`docker cp` out of the volume into `data/backups/<stack>/<tag>/`) → `docker compose down -v` → fresh `up` → prove the
data is gone → import the artifact into the new stack → restore → verify. `nslab backup` exports by default;
`nslab backup --restore --tag <t> --import` is the manual path.

| engine | export | destroy + fresh up | data after destroy | import + restore into the fresh stack | verified |
|---|---|---|---|---|---|
| Redis 8 | 336 MB in 0.6 s | 21 s + 13 s | 0 keys | 0.9 s + 23.2 s (RDB → AOF base, replica resync) | ✅ |
| MongoDB 7 | 51 MB in 0.1 s | 17 s + 22 s (rs.initiate again) | 0 docs | 0.5 s + 67.6 s (`mongorestore` recreates collections + indexes) | ✅ |
| Cassandra 5.0 | 379 MB in 1.05 s | 34 s + 56 s (3-node ring re-joins) | keyspace missing | 1.08 s + 39.3 s (keyspace + tables recreated from the harness DDL, then `nodetool import`) | ✅ |
| Neo4j 5 CE | 269 MB in 0.6 s | 12 s + 17 s | 0 nodes | 0.68 s + 37.83 s (`database load --overwrite-destination` onto the fresh store) | ✅ |
| Elasticsearch 9 (3 nodes) | 167 MB in 0.4 s | 5 s + 37 s | indices 404 | 0.7 s + 3.8 s (repository re-registered over the copied files, `_restore`) | ✅ |
| Neo4j 5 Enterprise cluster | 92 MB in 0.2 s | 13 s + 45 s (cluster re-forms) | database `lab` missing | 0.5 s + 19.4 s (`CREATE DATABASE lab ... seedURI` on the fresh cluster) | ✅ |
| LMDB | 829 MB in 3.2 s | files deleted | 0 | 0.4 s + 3.3 s | ✅ |
| RocksDB | 171 MB in 0.1 s | files deleted | 0 | 0.1 s + 0.1 s | ✅ |

What had to change for a *fresh* stack to accept a restore: Cassandra recreates the keyspace/tables before importing
(the export also carries `schema.cql` per table), Elasticsearch re-registers the `fs` repository over the copied-back
files (the repository is the artifact — copy the whole directory, snapshots share segments), every artifact is
`chmod a+rwX` after `docker cp` so the engine's own user can read/move it, and the harness re-runs `chmod 1777
/backups` on the new volume.

### Traps (each one produced a failed or empty restore before it was handled)
| engine | trap | handling |
|---|---|---|
| Redis | `appendonly yes` servers ignore `dump.rdb` at start-up; a fresh RDB in `/data` boots an empty dataset | install the RDB as the multi-part AOF **base** (`appendonlydir/appendonly.aof.1.base.rdb` + manifest line `file … seq 1 type b`) |
| Redis | stopping the primary for 11 s under Sentinel `down-after 2 s` = failover; the restored node rejoins as a replica and syncs the old data back | `SENTINEL SET mymaster down-after-milliseconds 600000` on all 3 Sentinels for the window, then reset; restore the whole group |
| MongoDB | `mongorestore --oplogReplay` refuses `--nsInclude/--nsExclude` | restore the whole archive; `dropDatabase` first so collections created after the backup do not survive `--drop` |
| Cassandra | `sstableloader` fails on `products` (`Could not parse type name vector<float, 8>`) — every stream session dies | server-side `nodetool import` per node (keeps SAI components) |
| Cassandra | `nodetool import -c` is `--no-invalidate-caches`; the import **moves** the SSTables and consumes the export | `--copy-data` |
| Cassandra | `du` of a table dir counts `snapshots/` (auto_snapshot on TRUNCATE) and `backups/` | `du --exclude=snapshots --exclude=backups` for "live" size |
| Neo4j CE | `STOP DATABASE neo4j` → "Unsupported administration command"; `neo4j-admin` refuses a missing `--to-path` dir; a root-run `load` leaves a root-owned store | stop the container, run `neo4j-admin` as `neo4j` in a helper container over the volumes, pre-create the dir |
| Neo4j EE | `CREATE DATABASE ... seedURI:'file://...'` → "No seed providers found to satisfy file:///…" (default providers are S3/cloud only); the seed file must be visible on every primary | `dbms.databases.seed_from_uri_providers=FileSeedProvider,...` (restart-time) + one `/backups` volume shared by all members; keep the dataset in its own database so it can be dropped and re-seeded |
| Elasticsearch | `DELETE lab_*` → 400 (`action.destructive_requires_name`); a fresh `/backups` volume is root-owned → "cannot create blob store" | delete indices by name; `chmod 1777 /backups` (the harness does it at `up`) |
| Elasticsearch | the repository is cumulative — `du` of it grows with every snapshot | report the snapshot's own `stats.total.size_in_bytes`; SLM retention prunes |
| all | a named volume shared with the engine user needs write permission | `prepare_backup_dirs()` runs `chmod 1777 /backups` after `up` |
| all | **the `backups` volume dies with `down -v`** — a backup that stays on the stack is not a backup | `export_backup()` copies every artifact to `data/backups/<stack>/<tag>/` (`nslab backup` does it by default; `nslab dr` proves the round trip) |

### Strategy per engine (what to run in production)
- **Redis**: `BGSAVE` on a replica (or `redis-cli --rdb`), copy the RDB off-box on a schedule sized to the RPO, keep AOF
  `everysec` for crash safety. Restore the whole replication group; disable/raise Sentinel `down-after` first.
- **MongoDB**: `mongodump --oplog` from a secondary for small/medium sets; Percona PBM for physical/incremental/PITR
  (`pbm backup`, `pbm restore --time`); keep the oplog window (`replSetResizeOplog`) longer than the backup interval.
- **Cassandra**: `nodetool snapshot --ttl` per node on cron (or Medusa), export in `<ks>/<table>/` layout, ship;
  `incremental_backups` between snapshots; `commitlog_archiving.properties` for PITR; restore with `nodetool import
  --copy-data` (or sstableloader when no vector/SAI types are involved), then `nodetool repair`.
- **Neo4j**: Community = scheduled downtime for `database dump`; Enterprise = `neo4j-admin database backup --type=full|
  differential` online, transaction-log PITR.
- **Elasticsearch**: `fs`/S3 repository + SLM policy (schedule + `expire_after/min_count/max_count`); restore beside the
  live index with `rename_pattern` when in doubt.
- **LMDB/RocksDB**: `env.copy(compact=True)` / `Checkpoint` from the application on a timer, ship the file/dir; restore
  = replace and reopen (the process must close the handle).

## 2. Logging

### What the phase does
1. Container: log driver, rotation options (`docker inspect`), bytes/lines logged so far, JSON share of the last 200 lines.
2. Server log: format and location as the engine reports it.
3. Slow-query log: set a 100 ms threshold (where possible), run a point read (must not appear) and a known-slow
   operation (must appear), read the entries back **through the engine's own interface**, check the slow op is there
   with its duration.
4. Audit log: enable, run three statements / two denied attempts, read back, disable.
5. Change the log level at run time, verify, revert.
6. Log volume produced during the phase.

### Results

| engine | container logs (this run) | server log | slow-query log | slow op → logged | audit log | run-time level |
|---|---|---|---|---|---|---|
| Redis 8 | json-file 50m×3; 50 KB primary, 37 KB replica, 2 KB/Sentinel; text | stdout text (`pid:role date level msg`) | `SLOWLOG` (`slowlog-log-slower-than` µs) + `LATENCY LATEST` (100 ms monitor) | `KEYS customers:*` 226 ms → 221 ms entry; `HGET` 0.3 ms not logged | ✅ `ACL LOG`: `reason=auth … AUTH nslab_auditor`, `reason=command … object=set` (denied SET by a read-only user) | ✅ `CONFIG SET loglevel debug` |
| MongoDB 7 | json-file; 8.1 MB / 7.6 MB / 7.6 MB per member; **JSON** | JSON on stdout; `getLog global` ring (1024 lines) | profiler level 1, `slowms 100`, `sampleRate 1.0` → `system.profile` + `"msg":"Slow query"` lines | `$group` over 1M events 1099 ms → 1097 ms (`COLLSCAN`, docsExamined 1M); `find _id` 0.9 ms not logged | ❌ Enterprise / Percona PSMDB | ✅ `logComponentVerbosity.command -1→1→-1` |
| Cassandra 5.0 | json-file; 1.0 / 1.0 / 2.3 MB per node; text | logback text: system.log, debug.log, gc.log (+ stdout) | `slow_query_log_timeout` **500 ms from cassandra.yaml** (`UPDATE system_views.settings` → "Modification is not supported"); MonitoringTask writes to debug.log every 5 s | `COUNT(*) FROM orders` 4.2 s client-side → `<SELECT * FROM lab.orders ALLOW FILTERING>, time 1736 msec` (the internal read command, per range); point read 2.4 ms not logged | ✅ `nodetool enableauditlog --included-keyspaces lab` → 3 entries via `auditlogviewer` (user, source, type, category, ks, scope, operation) | ✅ `nodetool setlogginglevel … TRACE` |
| Neo4j 5 CE | json-file; 7 KB (only neo4j.log reaches stdout); text | log4j2 files under /logs: neo4j.log, debug.log (20 MB×7), query.log, security.log, http.log | `db.logs.query.enabled=INFO` + `threshold=100ms` accepted (`SHOW SETTINGS`, dynamic) **but query.log stays 0 bytes even for a 3 s query** — Enterprise in practice; Community: `db.stats.collect/retrieve('QUERIES')` | label scan 925 ms → 723 ms execution in the collector (+compile); point read 85 ms (first compile) not ≥ threshold | ❌ security.log / query.log are Enterprise | ✅ edit `server-logs.xml` Root INFO→DEBUG, log4j2 `monitorInterval=30` applied it after 21 s (`dbms.setConfigValue` does not exist in CE) |
| Neo4j 5 EE cluster | json-file; ~2.5 KB per member; text | same files per member | `CALL dbms.setConfigValue('db.logs.query.threshold','100ms')` → **/logs/query.log on the member that ran the query** (works) | label scan 361 ms → `359 ms` line with parameters; point read 31 ms not logged | ✅ `security.log`: `ERROR [neo4j]: failed to log in: invalid principal or credentials` | ✅ `dbms.setConfigValue` 100→250→100 ms verified via `SHOW SETTINGS` |
| Elasticsearch 9 | json-file; 236 KB; **ECS JSON** | stdout ECS JSON (`event.dataset` server / slowlog datasets) | per-index `index.search.slowlog.threshold.query.warn=100ms` (+fetch, indexing), dynamic | `script_score` over 1M docs 249 ms → `took_millis 197` line with the query source; `GET _doc` 2.9 ms not logged | ❌ `xpack.security.audit.enabled` needs security (off here) | ✅ `PUT _cluster/settings logger.org.elasticsearch.snapshots=DEBUG` |
| LMDB | — | none | ❌ instrument the application | — | ❌ | ❌ |
| RocksDB | — | info `LOG` in the DB dir (rotated 50 MB×3, `rocksdb.stats` every 60 s) | ❌ instrument the application | — | ❌ | ❌ (open-time options) |

### What the numbers say
- **Every server engine has a usable slow-query facility, but the threshold is not always yours to set at run time**:
  Cassandra's 500 ms is a restart-time setting in 5.0.x; Neo4j Community's `db.logs.query.*` are accepted and silently
  do nothing — the query collector (`db.stats.*`) is the working alternative, in memory, no threshold.
- **The logged text ≠ your statement**: Cassandra logs the coordinator's internal read command per range (`SELECT *
  FROM lab.orders ALLOW FILTERING`, 1.7 s of a 4.2 s `COUNT(*)`), MongoDB logs the command document + `planSummary`,
  ES logs the query source with `took_millis`, Redis logs argv (truncated to `slowlog-entry-max-string-len`).
- **Open-source audit trails exist only in Cassandra (statements) and Redis (denials).** MongoDB, Neo4j and
  Elasticsearch put statement/authentication auditing behind Enterprise/security — a licensing decision, not a config one.
- **JSON out of the box: MongoDB and Elasticsearch.** Redis and Cassandra are text; Neo4j needs a `JsonTemplateLayout`.
- **Log volume varies 1000×**: MongoDB members wrote ~8 MB each during the lab (JSON, start-up + every phase), Cassandra
  1-2 MB per node, ES 236 KB, Redis 50 KB, Neo4j 7 KB to stdout (its real logs are files). Rotation at the container
  level (`x-logging`: json-file, 50 MB × 3, compressed) is the floor; engine-side rotation is the second layer.
- **Run-time verbosity changes work everywhere except embedded stores** — Redis/MongoDB/Cassandra/ES via a command,
  Neo4j via the log4j2 config reload (30 s).

### Harness logging
`results/logs/nslab.jsonl`: every console line plus `phase_start` / `phase_end` / `phase_failed` events with `run_id`,
`stack`, `phase`, `seconds` — stdlib `RotatingFileHandler` (20 MB × 5), `NSLAB_LOG_LEVEL`, `NSLAB_LOG_FILE`. This is the
pattern for applications on embedded engines: the store has no log, the application does.

## 3. The same strategies on Kubernetes
Everything above also runs on the k3d cluster with `NSLAB_PLATFORM=k3s` (`docs/kubernetes.md`): the backup phase's
helper containers become throw-away Pods over the same PersistentVolumeClaims, stop/start becomes scaling the
Deployment, exports use `kubectl cp` (through a busybox helper when the image has no `tar`, e.g. Elasticsearch), the DR
drill deletes the namespace and re-applies the manifests, and "schedule/retention" is a CronJob per stack writing into
the shared `backups` PVC (`k8s/gen-backup-addons.py`, `k8s/scenarios-backup.sh`). Logging on the cluster: kubelet
rotation (`container-log-max-size=50Mi`, 3 files) replaces the docker json-file driver; the engine-side slow-query /
audit / run-time-level round trips are unchanged. Results: `results/k3s/<engine>/latest.json`.

## 4. Where it lives
- `harness/nslab/phases/backup.py` (backup phase, export/import, DR drill), `harness/nslab/phases/logs.py` — the phases; `nslab backup` (exports by default), `nslab backup --restore --import`, `nslab dr`, `nslab slowlog` — ad-hoc.
- `harness/nslab/engines/*.py` — `BACKUP_STRATEGY`, `backup()`, `restore()`, `backup_incremental()`,
  `LOGGING_STRATEGY`, `slow_query_*`, `audit_log_*`, `log_level_runtime()`, `server_log_facts()` per adapter.
- `stacks/*/compose.yaml` — `x-logging` anchor, `backups` volume, slowlog/profiler/query-log settings, `path.repo`.
- `skills/nosql-backup-restore`, `skills/nosql-logging-observability` — the distilled how-to; vendored Redis,
  Percona (PBM, PSMDB, PMM) and Neo4j CLI skills under `skills/vendor/`.
