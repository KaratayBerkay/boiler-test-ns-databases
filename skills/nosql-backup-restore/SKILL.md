---
name: nosql-backup-restore
description: Design, run and verify backups for NoSQL engines - Redis (BGSAVE/RDB + AOF, restore under Sentinel), MongoDB (mongodump --oplog / mongorestore --oplogReplay, fsyncLock snapshots, PBM for PITR), Cassandra (nodetool snapshot + export, nodetool import vs sstableloader, incremental backups, commitlog PITR), Neo4j (neo4j-admin dump/load offline in Community, online backup in Enterprise), Elasticsearch (snapshot repositories, SLM schedules and retention, segment-level incremental restore), LMDB/RocksDB (env.copy, Checkpoint). Measured backup/restore times, artifact sizes, read-latency impact and downtime, plus a restore drill that proves a backup. Use when asked about backup, restore, snapshot, dump, RPO/RTO, point-in-time recovery, disaster recovery or "is my backup valid".
---

# NoSQL backup & restore

A backup you have not restored is a hypothesis. This skill is built from the lab's **backup phase**: one full backup with
the engine's own tooling while point reads keep running, then the dataset is mutated (500 probe docs added, 50 customers
deleted), the backup is restored, and the result is checked against a pre-backup fingerprint. Numbers below are from
that run (scale 1: 1.7M documents, one 28-core host; `results/<engine>/latest.json` → `phases.backup`).

## The strategy matrix (what "backup" means per engine)

| engine | tool the lab uses | online? | artifact consistency | incremental | PITR | schedule / retention |
|---|---|---|---|---|---|---|
| Redis 8 | `BGSAVE` → copy `dump.rdb` (AOF everysec in between) | yes (fork, CoW) | point-in-time | no (RDB is full) | no | `save` rules or cron; rotate files |
| MongoDB 7 | `mongodump --oplog --gzip --archive` | yes | point-in-time at dump end (oplog replayed) | no in Community (PBM: yes) | oplog window / PBM | cron on a secondary; PBM policies |
| Cassandra 5 | `nodetool flush` + `snapshot --ttl` + export per node | yes (hard links) | per node after flush; repair for cluster-wide | `nodetool enablebackup` | commitlog archiving | cron per node / Medusa; `--ttl`, `clearsnapshot` |
| Neo4j 5 Community | `neo4j-admin database dump` | **no** (server stopped) | transactional | Enterprise only | Enterprise only | maintenance window |
| Neo4j 5 Enterprise cluster | `neo4j-admin database backup --from=<member>:6362` | yes | transactional (tx log included) | `--type=differential` | transaction-log based | cron against a follower; restore = `CREATE DATABASE ... seedURI` on every primary |
| Elasticsearch 9 | `_snapshot` into an `fs`/S3 repository | yes | per-index point-in-time (segments) | **built in** (segment dedup) | no | **SLM** policy in-cluster |
| LMDB | `env.copy(compact=True)` | yes (read txn) | txn snapshot | no | no | application |
| RocksDB | `Checkpoint.create_checkpoint` | yes (hard links) | point-in-time | BackupEngine (C++) | no | application |

## Measured (backup → mutate → restore → verify)

| engine | backup | artifact (of live data) | point-read p50 during / baseline | restore | downtime | verified |
|---|---|---|---|---|---|---|
| Redis 8 | 5.8 s | 336 MB (77 % of `/data` incl. AOF; 590 MB used_memory) | 0.59 / 0.54 ms | 24 s: stop, install RDB as AOF base, start, replica full-resync | **11.4 s** | ✅ |
| MongoDB 7 | 13.6 s | 51 MB gzip (27 % of 188 MB storage+index; 267 MB logical) | 0.95 / 1.01 ms | 60 s online (`mongorestore --oplogReplay --drop`, 1.73M docs + index builds) | 0 | ✅ |
| Cassandra 5 | 8.6 s (3 nodes) | 380 MB = 3 × 126 MB node exports (= 379 MB live SSTables) | 1.42 / 1.63 ms | 21 s online (TRUNCATE + 45 `nodetool import --copy-data`) | 0 | ✅ |
| Neo4j 5 CE | 33.8 s **offline** | 306 MB dump (17 % of 1.84 GB store + tx logs) | reads fail while stopped | 42 s offline (`database load`) | **42 s** | ✅ |
| Neo4j 5 EE cluster | 19.8 s online | 92 MB backup (5 % of 1.71 GB) | 2.13 / 2.70 ms | 16 s: `DROP DATABASE` + `CREATE DATABASE … seedURI` (3 primaries seed from the shared file) | 15.8 s (database offline, cluster up) | ✅ |
| Elasticsearch 9 | 4.5 s | 166 MB snapshot (= 166 MB store) | 2.53 / 2.50 ms | 4.4 s online (delete + `_restore`) | 0 | ✅ |
| LMDB / RocksDB | 3.4 s / 0.03 s | 829 MB compact copy / 171 MB checkpoint (hard links) | unchanged | 6.8 s / 0.1 s (replace file or dir, reopen) | same (process-local) | ✅ |
| incremental | Cassandra 10.5 KB / ES 8.6 KB for 10 new docs; others n/a (full copies only) | | | | | |

Reading it: **online backups cost nothing measurable** on point reads (fork/hard-link/segment-copy designs); the price
is paid at restore — logical restores (MongoDB) rebuild indexes, offline restores (Redis with AOF, Neo4j Community)
take the service down for the RDB/store load, ES and Cassandra restore in seconds because the artifact *is* the storage
format.

## Restore drill (do this, not "the backup job is green")
1. **Fingerprint** cheap invariants: collection counts, a handful of known documents, a marker collection.
2. **Backup** with the engine's tool while a read probe runs (`ReadSampler` in the phase: p50 during vs baseline).
3. **Mutate**: add docs, delete docs, change a value — things a restore must undo.
4. **Restore** with an availability poller running (downtime = longest window of failed pings).
5. **Verify**: counts equal, deleted docs back, post-backup writes gone. Anything else is a failed backup.
`uv run --project harness nslab phase <stack> backup` (or `skills/nosql-backup-restore/scripts/backup_drill.py`) runs
exactly this; `nslab backup <stack>` / `--restore --tag` are the ad-hoc halves.

6. **Then do it again from nothing.** An in-place restore proves the artifact; it does not prove recovery. The lab's
   DR drill (`nslab dr <stack>`) exports the artifact off the stack, runs `docker compose down -v` (data *and* backups
   volumes gone), starts a fresh stack, proves the data is gone, imports the artifact and restores — all seven engines
   verified. The two things that only surfaced here: Cassandra needs its keyspace/tables recreated before
   `nodetool import`, and Elasticsearch needs the repository re-registered over the copied-back directory.
   Rule: **a backup that lives on the stack it protects is not a backup** — `nslab backup` exports to
   `data/backups/<stack>/<tag>/` by default; ship that directory somewhere else.

## Traps this lab hit (each cost a failed restore before it was handled)
- **Redis with AOF ignores `dump.rdb` at start-up.** An `appendonly yes` server loads only the multi-part AOF; a fresh
  RDB dropped into `/data` gives you an empty database. Install the RDB as the AOF *base* file
  (`appendonlydir/appendonly.aof.1.base.rdb` + a one-line manifest) — see `references/engine-recipes.md`.
- **Restoring a Sentinel-managed primary triggers a failover.** Stop = down-after (2 s) → promotion → the restored node
  rejoins as a replica and syncs the *old* data back. Raise `down-after-milliseconds` on every Sentinel for the window,
  restore, put it back. Restore the whole replication group, never one member.
- **`mongorestore --oplogReplay` refuses `--nsInclude/--nsExclude`.** Restore the whole archive; `dropDatabase` first,
  because `--drop` only drops collections present in the dump (collections created after the backup would survive).
- **Cassandra 5.0 `sstableloader` cannot parse `vector<float, 8>`** (its bundled driver: `Could not parse type name`) and
  every stream session fails for that table. `nodetool import --copy-data` (server-side, per node) restores every
  table including SAI index components — and **without `--copy-data` the import moves the files, consuming the
  export** (`-c` is `--no-invalidate-caches`, not copy). Export snapshots in `<keyspace>/<table>/` layout; `snapshot --ttl`
  for retention.
- **Neo4j Community rejects `STOP DATABASE`** ("Unsupported administration command"), so the only consistent dump is with
  the server stopped; run `neo4j-admin` as the `neo4j` user in a helper container over the same volumes, and create the
  target directory first (`--to-path` must exist).
- **Neo4j Enterprise `seedURI` needs the file seed provider** — the default `dbms.databases.seed_from_uri_providers` is
  S3/cloud only and `CREATE DATABASE … seedURI:'file://…'` fails with "No seed providers found"; add `FileSeedProvider`,
  put the backup on a volume every primary mounts, and keep the dataset in its own database so it can be dropped.
- **Elasticsearch `action.destructive_requires_name`** (default since 8.0) rejects `DELETE lab_*`; list and name the
  indices. The repository path must be under `path.repo` **and writable by uid 1000** (a fresh named volume is
  root-owned: `chmod 1777`).
- **`du` lies on Cassandra data dirs**: `auto_snapshot` (on TRUNCATE/DROP) and `backups/` are hard links inside the
  table directory — exclude them when you size "live data".

## RPO / RTO cheat sheet
- **RPO** = backup interval unless the engine has a continuous log you ship: MongoDB oplog (PBM), Cassandra commitlog
  archiving, Neo4j Enterprise transaction logs. Redis AOF and RocksDB WAL protect against crashes, not against "restore to
  10:42". Elasticsearch's RPO is the SLM schedule.
- **RTO** = restore time + index/replica rebuild + the app's reconnect: measured here 4 s (ES) → 21-24 s (Cassandra,
  Redis) → 42 s (Neo4j CE) → 60 s (MongoDB, index builds). Restores of logical dumps scale with document count and
  index count; physical/segment restores scale with bytes.
- Keep the artifact off the node (`/backups` is a docker volume here — ship it), test restore into a *fresh* stack
  quarterly, and record the fingerprint with the artifact so verification is not guesswork.

## Related
- `references/engine-recipes.md` — the exact commands, per engine, with sizes and timings.
- `scripts/backup_drill.py` — the drill against a running lab stack.
- Vendored: `skills/vendor/percona-lab__skills__pbm-recipes` (MongoDB PBM: logical/physical/incremental/PITR),
  `skills/vendor/neo4j-contrib__neo4j-skills__cli-tools` (`neo4j-admin` dump/load/backup/restore/copy reference).
- Sibling skills: `nosql-replication-failover` (replicas are not backups), `docker-compose-nosql` (volumes, init).
