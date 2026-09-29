# Backup & restore

One full backup with the engine's own tooling while point reads keep running, then the dataset is mutated
(500 probe docs added, 50 customers deleted) and the backup is restored. "Verified" means counts, the deleted
customers and the probe collection all match the pre-backup fingerprint and the post-backup writes are gone.
Downtime is the longest window in which the primary did not answer. "Survives down -v" is the disaster-recovery
drill: the artifact is exported off the stack, the stack is destroyed with `docker compose down -v`, a fresh
one is started, the artifact imported and restored, and the fingerprint checked again.

| engine | backup | online | backup s | artifact | % of data | read p50 during/base | restore | restore s | downtime | verified | incremental | survives down -v |
|---|---|---|---:|---:|---:|---|---|---:|---:|---|---|
| MongoDB 7.0 | mongodump --oplog --gzip --archive | yes | 13.55 | 50.8 MB | 27.1 | 0.95 ms / 1.01 ms | dropDatabase + mongorestore --oplogReplay --drop | 59.53 | 0.00 ms | ✔ | unsupported | ✔ (export 50.8 MB, fresh up 22.1 s, restore 67.55 s) |
| MongoDB 7.0 (k3s) | mongodump --oplog --gzip --archive | yes | 15.86 | 50.8 MB | 4.7 | 1.23 ms / 1.24 ms | dropDatabase + mongorestore --oplogReplay --drop | 76.0 | 0.00 ms | ✔ | unsupported | ✔ (export 50.8 MB, fresh up 31.1 s, restore 68.84 s) |
| LMDB | lmdb env.copy(compact=True) inside a read txn | yes | 3.35 | 828.9 MB | 100.0 | 0.07 ms / 0.06 ms | close env; copy backup file over lab.mdb; reopen | 6.79 | 6.79 s | ✔ | unsupported | ✔ (export 828.9 MB, fresh up 0.0 s, restore 3.33 s) |
| RocksDB | rocksdict Checkpoint.create_checkpoint (hard links) after flush | yes | 0.03 | 170.8 MB | 50.4 | 0.06 ms / 0.09 ms | close; replace lab.rocks with the checkpoint dir; reopen | 0.1 | 104 ms | ✔ | unsupported | ✔ (export 170.8 MB, fresh up 0.0 s, restore 0.09 s) |
| Neo4j 5 | stop server; neo4j-admin database dump; start (Community: offline) | no | 33.79 | 306.2 MB | 16.7 | 3.60 ms / 4.17 ms | stop server; neo4j-admin database load --overwrite-destination; start | 42.16 | 42.08 s | ✔ | unsupported | ✔ (export 269.2 MB, fresh up 17.3 s, restore 37.83 s) |
| Neo4j 5 (k3s) | stop server; neo4j-admin database dump; start (Community: offline) | no | 53.24 | 394.4 MB | 16.6 | 1.82 ms / 2.35 ms | stop server; neo4j-admin database load --overwrite-destination; start | 63.56 | 71.57 s | ✔ | unsupported | ✔ (export 269.2 MB, fresh up 27.9 s, restore 52.92 s) |
| Neo4j 5 Enterprise cluster | neo4j-admin database backup lab --from=neo4j1:6362 --type=full (online, Enterprise) | yes | 19.75 | 92.3 MB | 5.4 | 2.13 ms / 2.70 ms | DROP DATABASE + CREATE DATABASE ... OPTIONS {existingData:'use', seedURI:'file://.../<tag>.backup'} WAIT (every primary seeds from the shared file) | 15.88 | 15.81 s | ✔ | unsupported | ✔ (export 92.3 MB, fresh up 45.3 s, restore 19.38 s) |
| Neo4j 5 Enterprise cluster (k3s) | neo4j-admin database backup lab --from=neo4j1:6362 --type=full (online, Enterprise) | yes | 25.0 | 91.7 MB | 5.7 | 2.45 ms / 2.84 ms | DROP DATABASE + CREATE DATABASE ... OPTIONS {existingData:'use', seedURI:'file://.../<tag>.backup'} WAIT (every primary seeds from the shared file) | 16.41 | 16.17 s | ✔ | unsupported | ✔ (export 91.7 MB, fresh up 54.4 s, restore 15.48 s) |
| Redis 8 | BGSAVE + copy dump.rdb | yes | 5.78 | 335.8 MB | 77.3 | 0.59 ms / 0.54 ms | stop; RDB -> appendonlydir base + manifest; start; replicas full-resync | 24.01 | 11.37 s | ✔ | unsupported | ✔ (export 335.8 MB, fresh up 12.6 s, restore 23.17 s) |
| Redis 8 (k3s) | BGSAVE + copy dump.rdb | yes | 7.26 | 411.2 MB | 25.7 | 0.69 ms / 0.73 ms | stop; RDB -> appendonlydir base + manifest; start; replicas full-resync | 84.29 | 20.09 s | ✔ | unsupported | ✔ (export 335.8 MB, fresh up 22.5 s, restore 57.31 s) |
| Elasticsearch 9 | PUT _snapshot/nslab/<tag> (fs repository, compress) | yes | 7.16 | 245.9 MB | 41.6 | 2.82 ms / 2.85 ms | DELETE the 8 lab_* indices (by name) + POST _snapshot/nslab/<tag>/_restore | 6.94 | 0.00 ms | ✔ | 66 KB in 0.15 s | ✔ (export 166.5 MB, fresh up 37.0 s, restore 3.8 s) |
| Elasticsearch 9 (k3s) | PUT _snapshot/nslab/<tag> (fs repository, compress) | yes | 7.74 | 310.3 MB | 54.3 | 2.97 ms / 2.95 ms | DELETE the 8 lab_* indices (by name) + POST _snapshot/nslab/<tag>/_restore | 10.39 | 0.00 ms | ✔ | 10 KB in 0.48 s | ✔ (export 166.7 MB, fresh up 39.3 s, restore 3.88 s) |
| Cassandra 5.0 | nodetool flush + snapshot --ttl 3h on every node, export to /backups (sstableloader layout) | yes | 8.56 | 379.5 MB | 100.0 | 1.42 ms / 1.63 ms | TRUNCATE tables + nodetool import --copy-data of each node's own snapshot export (parallel per node) | 21.38 | 0.00 ms | ✔ | 11 KB in 3.15 s | ✔ (export 378.6 MB, fresh up 55.5 s, restore 39.3 s) |
| Cassandra 5.0 (k3s) | nodetool flush + snapshot --ttl 3h on every node, export to /backups (sstableloader layout) | yes | 11.72 | 509.7 MB | 106.6 | 1.86 ms / 1.82 ms | TRUNCATE tables + nodetool import --copy-data of each node's own snapshot export (parallel per node) | 27.8 | 0.00 ms | ✔ | 5 KB in 3.71 s | ✔ (export 378.5 MB, fresh up 62.1 s, restore 44.67 s) |

## Backup strategy notes

### MongoDB 7.0 (data 187.8 MB)

- **tool**: mongodump --oplog --gzip --archive (logical, cluster-consistent thanks to the oplog captured during the dump); physical alternative: fsyncLock + volume snapshot
- **consistency**: point-in-time at the end of the dump (--oplogReplay applies the oplog captured while dumping)
- **restore**: online: dropDatabase, then mongorestore --oplogReplay --drop against the primary (secondaries replicate the restore); --oplogReplay cannot be combined with namespace filters
- **incremental**: none in Community tooling; Percona PBM does physical + incremental + selective restores
- **pitr**: oplog-based — keep the oplog window (replSetResizeOplog) longer than the backup interval; PBM/Ops Manager tail it continuously
- **schedule**: cron / CronJob running mongodump on a secondary (readPreference=secondary) or PBM's scheduler
- **retention**: external: archives are self-contained files; PBM has retention policies

### MongoDB 7.0 (k3s) (data 1.09 GB)

- **tool**: mongodump --oplog --gzip --archive (logical, cluster-consistent thanks to the oplog captured during the dump); physical alternative: fsyncLock + volume snapshot
- **consistency**: point-in-time at the end of the dump (--oplogReplay applies the oplog captured while dumping)
- **restore**: online: dropDatabase, then mongorestore --oplogReplay --drop against the primary (secondaries replicate the restore); --oplogReplay cannot be combined with namespace filters
- **incremental**: none in Community tooling; Percona PBM does physical + incremental + selective restores
- **pitr**: oplog-based — keep the oplog window (replSetResizeOplog) longer than the backup interval; PBM/Ops Manager tail it continuously
- **schedule**: cron / CronJob running mongodump on a secondary (readPreference=secondary) or PBM's scheduler
- **retention**: external: archives are self-contained files; PBM has retention policies

### LMDB (data 829.0 MB)

- **tool**: env.copy(path, compact=True) (== mdb_copy -c): a consistent copy taken inside one read transaction while the app keeps writing
- **consistency**: transaction snapshot (MVCC read txn pins the pages)
- **restore**: offline for the process: close the environment, replace the data file, reopen
- **incremental**: none (single file; copy is always full — compact drops free pages)
- **pitr**: none
- **schedule**: the application (or mdb_copy from cron; the file can be copied while open)
- **retention**: external

### RocksDB (data 339.1 MB)

- **tool**: Checkpoint.create_checkpoint(dir): hard-links the immutable SSTs + copies MANIFEST/CURRENT/WAL — an openable DB in milliseconds; BackupEngine (C++) adds incremental backups
- **consistency**: point-in-time (checkpoint of the current version)
- **restore**: offline for the process: close, replace the DB directory with the checkpoint, reopen
- **incremental**: BackupEngine only (not exposed by rocksdict); checkpoints share SST files through hard links
- **pitr**: none (the WAL is for crash recovery, not replay to a timestamp)
- **schedule**: the application (checkpoint on a timer, then rsync/upload the directory)
- **retention**: delete old checkpoint dirs (SSTs are freed when the last link goes)

### Neo4j 5 (data 1.84 GB)

- **tool**: neo4j-admin database dump (single-file archive) — Community has no online backup, and STOP DATABASE is rejected, so the server is stopped for the dump
- **consistency**: transactionally consistent (store closed while dumping)
- **restore**: offline: stop, neo4j-admin database load --overwrite-destination=true, start (both run as the neo4j user in a helper container over the data volume)
- **incremental**: Enterprise only (neo4j-admin database backup --type=differential)
- **pitr**: Enterprise only (transaction-log based); Community = last full dump
- **schedule**: a maintenance window per dump (cron: stop, dump, start) — or move to Enterprise for online full/differential backups
- **retention**: external: dump files under /backups/<tag>/

### Neo4j 5 (k3s) (data 2.38 GB)

- **tool**: neo4j-admin database dump (single-file archive) — Community has no online backup, and STOP DATABASE is rejected, so the server is stopped for the dump
- **consistency**: transactionally consistent (store closed while dumping)
- **restore**: offline: stop, neo4j-admin database load --overwrite-destination=true, start (both run as the neo4j user in a helper container over the data volume)
- **incremental**: Enterprise only (neo4j-admin database backup --type=differential)
- **pitr**: Enterprise only (transaction-log based); Community = last full dump
- **schedule**: a maintenance window per dump (cron: stop, dump, start) — or move to Enterprise for online full/differential backups
- **retention**: external: dump files under /backups/<tag>/

### Neo4j 5 Enterprise cluster (data 1.71 GB)

- **tool**: neo4j-admin database backup --from=<member>:6362 (online, from any member; --type=full|differential) into the shared /backups volume
- **consistency**: transactionally consistent (the member's transaction log is included; `backup --type=differential` chains on the last full)
- **restore**: cluster-wide re-seed: DROP DATABASE lab; CREATE DATABASE lab OPTIONS {existingData:'use', seedURI:'file:///backups/<tag>/<file>.backup'} WAIT — every primary seeds from the same file
- **incremental**: differential backups (--type=differential) and `neo4j-admin database aggregate-backup` to fold them
- **pitr**: transaction-log based (restore a full + differential chain up to a transaction id) — Enterprise
- **schedule**: cron/CronJob running the online backup against a follower; no downtime
- **retention**: external: prune backup directories, aggregate chains

### Neo4j 5 Enterprise cluster (k3s) (data 1.61 GB)

- **tool**: neo4j-admin database backup --from=<member>:6362 (online, from any member; --type=full|differential) into the shared /backups volume
- **consistency**: transactionally consistent (the member's transaction log is included; `backup --type=differential` chains on the last full)
- **restore**: cluster-wide re-seed: DROP DATABASE lab; CREATE DATABASE lab OPTIONS {existingData:'use', seedURI:'file:///backups/<tag>/<file>.backup'} WAIT — every primary seeds from the same file
- **incremental**: differential backups (--type=differential) and `neo4j-admin database aggregate-backup` to fold them
- **pitr**: transaction-log based (restore a full + differential chain up to a transaction id) — Enterprise
- **schedule**: cron/CronJob running the online backup against a follower; no downtime
- **retention**: external: prune backup directories, aggregate chains

### Redis 8 (data 434.1 MB)

- **tool**: BGSAVE (fork, point-in-time RDB) then copy dump.rdb off the box; AOF everysec keeps the gap between snapshots small
- **consistency**: point-in-time (fork + copy-on-write); one file per instance
- **restore**: offline: stop, install the RDB as the multipart-AOF base file (appendonlydir/ + manifest) so an AOF-enabled server loads it, start; replicas full-resync
- **incremental**: none — RDB is always full; BGREWRITEAOF compacts the AOF
- **pitr**: no (AOF replays to the last fsync only)
- **schedule**: `save <seconds> <changes>` rules or a cron job calling BGSAVE + copy (Sentinel/replica-aware)
- **retention**: external: rotate the copied RDB files

### Redis 8 (k3s) (data 1.60 GB)

- **tool**: BGSAVE (fork, point-in-time RDB) then copy dump.rdb off the box; AOF everysec keeps the gap between snapshots small
- **consistency**: point-in-time (fork + copy-on-write); one file per instance
- **restore**: offline: stop, install the RDB as the multipart-AOF base file (appendonlydir/ + manifest) so an AOF-enabled server loads it, start; replicas full-resync
- **incremental**: none — RDB is always full; BGREWRITEAOF compacts the AOF
- **pitr**: no (AOF replays to the last fsync only)
- **schedule**: `save <seconds> <changes>` rules or a cron job calling BGSAVE + copy (Sentinel/replica-aware)
- **retention**: external: rotate the copied RDB files

### Elasticsearch 9 (data 590.7 MB)

- **tool**: snapshot API into a registered repository (type fs under path.repo; S3/GCS/Azure repositories in production) — segment-level incremental
- **consistency**: per-index point-in-time at snapshot start (immutable Lucene segments; the translog is not included)
- **restore**: online: delete (or close) the indices, POST _snapshot/<repo>/<snap>/_restore (rename_pattern to restore beside the live index); the fs repository must be the same shared path on every node
- **incremental**: built in — a repository stores each segment once, the 2nd snapshot uploads only new segments (snapshot.stats.incremental)
- **pitr**: no; the SLM schedule is the RPO (searchable snapshots / CCR for other goals)
- **schedule**: SLM policy (cron schedule + retention) executed by the cluster itself — created by the backup phase
- **retention**: SLM retention (expire_after, min_count, max_count) + DELETE _snapshot/<repo>/<snap>

### Elasticsearch 9 (k3s) (data 571.9 MB)

- **tool**: snapshot API into a registered repository (type fs under path.repo; S3/GCS/Azure repositories in production) — segment-level incremental
- **consistency**: per-index point-in-time at snapshot start (immutable Lucene segments; the translog is not included)
- **restore**: online: delete (or close) the indices, POST _snapshot/<repo>/<snap>/_restore (rename_pattern to restore beside the live index); the fs repository must be the same shared path on every node
- **incremental**: built in — a repository stores each segment once, the 2nd snapshot uploads only new segments (snapshot.stats.incremental)
- **pitr**: no; the SLM schedule is the RPO (searchable snapshots / CCR for other goals)
- **schedule**: SLM policy (cron schedule + retention) executed by the cluster itself — created by the backup phase
- **retention**: SLM retention (expire_after, min_count, max_count) + DELETE _snapshot/<repo>/<snap>

### Cassandra 5.0 (data 379.5 MB)

- **tool**: nodetool flush + nodetool snapshot -t <tag> --ttl 3h (hard-linked SSTables, instant, per node) then export the snapshot dirs to /backups/<tag>/<node>/lab/<table>/
- **consistency**: per-node crash-consistent after the flush; cluster-wide consistency comes from repair after restore (or a coordinated snapshot)
- **restore**: online: TRUNCATE every table, then each node runs nodetool import --copy-data on its own exported snapshot (server-side, SAI components included; without --copy-data the export is moved, i.e. consumed); sstableloader streams to the owning replicas but its driver cannot parse vector<float, 8> on 5.0.x
- **incremental**: incremental_backups (nodetool enablebackup): each flushed SSTable is hard-linked into <table>/backups/ — ship those as they appear
- **pitr**: commitlog archiving (archive_command + restore_point_in_time) — configured in commitlog_archiving.properties
- **schedule**: cron on every node (nodetool snapshot) or Medusa / Scylla Manager; --ttl makes snapshots self-expiring
- **retention**: nodetool clearsnapshot / snapshot --ttl; hard links cost nothing until compaction rewrites the SSTables

### Cassandra 5.0 (k3s) (data 478.2 MB)

- **tool**: nodetool flush + nodetool snapshot -t <tag> --ttl 3h (hard-linked SSTables, instant, per node) then export the snapshot dirs to /backups/<tag>/<node>/lab/<table>/
- **consistency**: per-node crash-consistent after the flush; cluster-wide consistency comes from repair after restore (or a coordinated snapshot)
- **restore**: online: TRUNCATE every table, then each node runs nodetool import --copy-data on its own exported snapshot (server-side, SAI components included; without --copy-data the export is moved, i.e. consumed); sstableloader streams to the owning replicas but its driver cannot parse vector<float, 8> on 5.0.x
- **incremental**: incremental_backups (nodetool enablebackup): each flushed SSTable is hard-linked into <table>/backups/ — ship those as they appear
- **pitr**: commitlog archiving (archive_command + restore_point_in_time) — configured in commitlog_archiving.properties
- **schedule**: cron on every node (nodetool snapshot) or Medusa / Scylla Manager; --ttl makes snapshots self-expiring
- **retention**: nodetool clearsnapshot / snapshot --ttl; hard links cost nothing until compaction rewrites the SSTables

