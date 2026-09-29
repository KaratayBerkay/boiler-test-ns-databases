# Backup & restore recipes per engine (verified in the ns-databases lab, Sept 2026)

Every recipe below is what `harness/nslab/engines/<adapter>.py` `backup()` / `restore()` actually runs. Paths assume the
lab's compose stacks (a named volume mounted at `/backups` in every database container, `chmod 1777` once).

## Getting the artifact off the stack (every engine)
```bash
docker cp nslab-redis-primary:/backups/x.rdb        data/backups/redis/x/        # file artifacts (Redis, MongoDB)
docker cp nslab-cassandra1:/backups/x               data/backups/cassandra/x/    # directory exports (Cassandra, Neo4j)
docker cp nslab-es:/backups/nslab                   data/backups/elasticsearch/x/  # ES: the whole repository (snapshots share segments)
# back into a fresh stack: docker cp <host path> <container>:/backups/ && docker exec -u 0 <container> chmod -R a+rwX /backups/<name>
```
`nslab backup <stack>` does the export automatically, `nslab backup <stack> --restore --tag x --import` the import, and
`nslab dr <stack>` the full backup → export → `down -v` → fresh `up` → import → restore → verify drill.

## Redis 8 (RDB snapshot; AOF everysec for the gap)
```bash
# backup (online, ~6 s for 1.7M keys / 590 MB used_memory; fork 22 ms, CoW 4 MB)
redis-cli BGSAVE                                     # or SAVE on a replica; --save "" only disables the automatic rules
until [ "$(redis-cli INFO persistence | tr -d '\r' | awk -F: '/rdb_bgsave_in_progress/{print $2}')" = 0 ]; do sleep 0.1; done
redis-cli INFO persistence | grep rdb_last_bgsave_status        # must be ok
docker exec nslab-redis-primary cp /data/dump.rdb /backups/$(date +%Y%m%d-%H%M%S).rdb
# alternative without touching the server's disk: redis-cli --rdb /backups/x.rdb (SYNC over the replication protocol)

# restore (offline): an AOF-enabled server IGNORES dump.rdb at start-up, so install the RDB as the multi-part AOF base
for s in 1 2 3; do docker exec nslab-redis-sentinel$s redis-cli -p 26379 SENTINEL SET mymaster down-after-milliseconds 600000; done
docker stop -t 15 nslab-redis-primary
docker run --rm -v nslab-redis_redis-primary:/data -v nslab-redis_backups:/backups redis:8 sh -c '
  rm -rf /data/appendonlydir /data/dump.rdb && mkdir /data/appendonlydir &&
  cp /backups/x.rdb /data/appendonlydir/appendonly.aof.1.base.rdb &&
  printf "file appendonly.aof.1.base.rdb seq 1 type b\n" > /data/appendonlydir/appendonly.aof.manifest &&
  chown -R redis:redis /data/appendonlydir'
docker start nslab-redis-primary                     # loads the base RDB, creates a fresh incr AOF; replica full-resyncs (new replid)
for s in 1 2 3; do docker exec nslab-redis-sentinel$s redis-cli -p 26379 SENTINEL SET mymaster down-after-milliseconds 2000; done
```
Why the Sentinel step: with `down-after 2 s` the stop would trigger a failover, the replica would be promoted, and the
restored primary would rejoin as a replica and *sync the un-restored data back*. Raise `down-after` (or `SENTINEL SET
mymaster failover-timeout`), restore, then put it back. Restore the whole set; never restore one member.

## MongoDB 7 (mongodump/mongorestore with the oplog)
```bash
# backup (online, ~13.6 s; 51 MB gzip archive for 188 MB storage+index (267 MB logical); point-read p50 unchanged)
docker exec nslab-mongo1 mongodump --host=localhost:27017 --oplog --gzip --archive=/backups/x.archive.gz
#   --oplog: dump the oplog entries written *during* the dump -> consistent point in time at dump end
#   run it against a secondary in production (--host secondary --readPreference=secondary) to spare the primary
# physical alternative (volume snapshot): writes blocked only while the snapshot is taken
mongosh --eval 'db.fsyncLock()'; <take the volume snapshot>; mongosh --eval 'db.fsyncUnlock()'   # writes blocked 2.1 s for cp -a of 1.09 GB (data + oplog + journal) here

# restore (online, ~60 s: 1.73M documents + index rebuilds; the replica set replicates it)
mongosh --eval 'db.getSiblingDB("lab").dropDatabase()'          # collections created after the backup would survive --drop
docker exec nslab-mongo1 mongorestore --host=localhost:27017 --gzip --archive=/backups/x.archive.gz --oplogReplay --drop
#   --oplogReplay refuses --nsInclude/--nsExclude ("cannot use --oplogReplay with includes specified"): restore all or nothing
```
PITR: the oplog window (`rs.printReplicationInfo()`, `replSetResizeOplog`) must exceed the backup interval; continuous
oplog tailing with restore-to-timestamp is Percona PBM (`pbm backup`, `pbm restore --time`) — see the vendored
`percona-lab__skills__pbm-recipes`.

## Cassandra 5.0 (nodetool snapshot + nodetool import)
```bash
# backup (online, 8.6 s for 3 nodes incl. export; the snapshot itself is hard links, ~2 s per node incl. flush)
for n in 1 2 3; do
  docker exec nslab-cassandra$n nodetool flush lab
  docker exec nslab-cassandra$n nodetool snapshot -t nightly --ttl 3h lab        # --ttl: self-expiring (4.1+)
  # export in <keyspace>/<table>/ layout (what sstableloader and nodetool import expect)
  docker exec nslab-cassandra$n sh -c 'for d in /var/lib/cassandra/data/lab/*/snapshots/nightly; do
     tb=$(basename $(dirname $(dirname $d))); tb=${tb%-*}; mkdir -p /backups/nightly/node$n/lab/$tb; cp -a "$d"/. /backups/nightly/node$n/lab/$tb/; done'
done
docker exec nslab-cassandra1 nodetool listsnapshots            # true size vs size on disk

# restore (online, ~21 s: TRUNCATE 15 tables (0.8 s) + 45 nodetool import calls, ~20 s per node in parallel)
cqlsh -e 'TRUNCATE lab.customers; ...'                          # every table (auto_snapshot keeps a safety copy)
for n in 1 2 3; do for tb in $(ls /backups/nightly/node$n/lab); do
  docker exec nslab-cassandra$n nodetool import --copy-data lab $tb /backups/nightly/node$n/lab/$tb; done & done; wait
#   --copy-data, not -c: `-c` is --no-invalidate-caches and the import then MOVES the files out of the export
```
`sstableloader -d <node> /backups/nightly/node1/lab/products` (client-side streaming to all replicas) **fails on
5.0.x for a table with a `vector<float, 8>` column** — its bundled driver reports `Could not parse type name
vector<float, 8>` and every stream session dies. `nodetool import` is server-side, keeps the SAI index components and
works. With RF=N every node's snapshot is a full copy; with RF<N restore every node's own export (or stream with
sstableloader for tables without vectors).
Incremental: `nodetool enablebackup` → each flushed SSTable is hard-linked into `<table>/backups/` (5 KB for 10 rows
here); ship those and `nodetool disablebackup`/clean when done. PITR: `commitlog_archiving.properties`.
Retention: `nodetool clearsnapshot -t nightly` or the `--ttl`.

## Neo4j 5 Community (offline dump/load)
```bash
# Community: STOP DATABASE is "Unsupported administration command", online backup is Enterprise -> stop the server
docker exec -u 0 nslab-neo4j sh -c 'mkdir -p /backups/x && chmod 777 /backups/x'   # neo4j-admin does not create the dir
docker stop -t 15 nslab-neo4j
docker run --rm -u neo4j --entrypoint neo4j-admin -v nslab-neo4j_neo4j-data:/data -v nslab-neo4j_backups:/backups \
   neo4j:5-community database dump neo4j --to-path=/backups/x          # 306 MB dump for a 1.77 GB store, ~6 s
docker start nslab-neo4j                                                # ~10-20 s until Bolt answers; total downtime ~32 s
# restore: same dance with `database load neo4j --from-path=/backups/x --overwrite-destination=true`
```
Run the helper as `neo4j` (uid 7474): a root-run `load` leaves the store root-owned and the server cannot write it.
Enterprise: `neo4j-admin database backup --type=full|differential` online + `database restore`, transaction-log PITR.

## Elasticsearch 9 (snapshot repository + SLM)
```bash
# once: path.repo=/backups in elasticsearch.yml / env; the volume must be writable by uid 1000
curl -XPUT localhost:9200/_snapshot/nslab -H 'Content-Type: application/json' \
  -d '{"type":"fs","settings":{"location":"/backups/nslab","compress":true}}'          # S3: type "s3", bucket, base_path
# backup (online, 4.5 s for 8 indices / 1.7M docs, 166 MB = the store size; every later snapshot copies only new segments: 8.6 KB in 0.09 s here)
curl -XPUT 'localhost:9200/_snapshot/nslab/x?wait_for_completion=true' -H 'Content-Type: application/json' \
  -d '{"indices":"lab_*","include_global_state":false}'
curl 'localhost:9200/_snapshot/nslab/x/_status'                     # stats.total vs stats.incremental
# schedule + retention inside the cluster
curl -XPUT localhost:9200/_slm/policy/nslab-nightly -H 'Content-Type: application/json' -d '{"schedule":"0 30 1 * * ?",
  "name":"<nslab-{now/d}>","repository":"nslab","config":{"indices":["lab_*"],"include_global_state":false},
  "retention":{"expire_after":"7d","min_count":3,"max_count":30}}'
# restore (online, ~4 s): destructive_requires_name (8.0+) rejects wildcard deletes -> name the indices
curl -XDELETE localhost:9200/lab_customers,lab_orders,...
curl -XPOST 'localhost:9200/_snapshot/nslab/x/_restore?wait_for_completion=true' -H 'Content-Type: application/json' \
  -d '{"indices":"lab_*","include_global_state":false}'             # or rename_pattern/rename_replacement to restore beside
```

## LMDB / RocksDB (embedded)
```python
env.copy("/backups/lab.mdb", compact=True)         # LMDB: consistent copy inside one read txn (== mdb_copy -c), online
Checkpoint(db).create_checkpoint("/backups/ckpt")  # RocksDB (rocksdict): hard-linked SSTs + MANIFEST/CURRENT/WAL, ms
# restore = close the handle, replace the file/dir, reopen (the process is "down" for that window)
```
RocksDB `BackupEngine` (C++/Java) adds incremental backups on top of checkpoints; Litestream-style WAL shipping is
external to both.
