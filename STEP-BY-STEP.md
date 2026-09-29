# Step by step: testing everything in this lab

Every command below is run from this directory (`ns-databases/`). `uv run --project harness nslab …` is the harness CLI;
`cd harness && uv run nslab …` is the same thing.

## 0. What you need
- Docker Engine with Compose v2+ (`docker compose version`), ~20 GB free disk for images and volumes, and a machine with
  spare CPU: the numbers in `results/` came from a 28-core / 125 GB host with every container on that one machine.
- Python 3.12 and `uv` (https://docs.astral.sh/uv/).

## 1. Install the harness (once)
```bash
uv sync --project harness                    # core drivers (redis, pymongo, cassandra-driver, neo4j, httpx, lmdb, rocksdict)
uv sync --project harness --all-extras       # + couchbase, aerospike, qdrant, weaviate, milvus, surreal, arango, etcd (when you add those stacks)
uv run --project harness nslab --help
uv run --project harness nslab list          # the configured stacks and their families/drivers
```

## 2. Generate the dataset (once)
```bash
uv run --project harness nslab gen --scale 1   # ~35 s, writes data/scale-1/*.ndjson (seeded, identical every time)
```
Scale 1 = 20k customers, 200 categories, 5k products, 200k orders, 488k order items, 1M events, 20k inventory rows —
the **same values** as rd-databases, emitted as JSON documents with extra `customers.location` (geo), `products.embedding`
(8-dim vector) and `orders.items` (embedded) fields. `--scale 0.3` makes a smaller set.

## 3. Pull the images (once)
```bash
scripts/pull-images.sh                       # every image referenced by stacks/*/compose.yaml, via mirror.gcr.io
scripts/pull-images.sh redis:8 mongo:7       # or just some
```
The script routes Docker Hub images through Google's mirror (anonymous Docker Hub allows only 100 manifest pulls / 6 h).
Elasticsearch comes from `docker.elastic.co` directly. Embedded engines (lmdb, rocksdb) need no image.

## 4. Run one engine end to end
```bash
uv run --project harness nslab run redis              # up -> all phases -> down (5-15 min)
uv run --project harness nslab run redis --failover   # also kills the primary and measures failover (stack torn down afterwards)
```
What happens, in order:

| phase | measures | typical time |
|---|---|---|
| `load` | schema/collection/index creation + bulk load through the engine's fastest path | 10 s - 4 min |
| `capabilities` | 33 feature probes (indexes, transactions, consistency, aggregation, joins, traversal, streams, scripting, backup/PITR, slow-query/audit/structured logs …) | 5-30 s |
| `bench` | 28 catalog operations, p50/p95 over ≥10 runs after 2 warm-ups, result checksums | 1-5 min |
| `optimize` | 9 before/after experiments (index vs scan, keyset vs offset, batching, multi-get, embedding, durability, consistency, pooling) | 2-6 min |
| `connections` | connect latency, connection storms to the server limit, throughput vs 1/4/16/32/64 processes, pool saturation, compare-and-set contention, proxy read/write split | 3-6 min |
| `loadtest` | multi-connection bulk insert (N processes × 1000-doc batches) with exact cgroup CPU/memory per container | 2-6 min |
| `backup` | one full backup with the engine's tooling while point reads run, mutate, restore, verify (counts + deleted docs back + post-backup writes gone), incremental second backup | 1-3 min |
| `logging` | container log driver/rotation + volume, server log format, slow-query log round trip (threshold, fast op not logged, slow op logged), audit-log round trip, run-time log-level change | 10 s - 2 min |
| `replication` | topology, visibility lag primary→replica, replica write rejection, catch-up after 20k docs, read scaling; `--failover` adds kill-promote-measure (runs last: it demotes the primary) | 1-4 min |

Useful variations:
```bash
uv run --project harness nslab run mongodb --phases load,bench --keep     # subset of phases, leave the stack running
uv run --project harness nslab up cassandra                                # just start a stack
uv run --project harness nslab phase cassandra optimize                    # one phase against a running stack
uv run --project harness nslab run redis --no-up --keep --phases connections --storms 50,500,2000
uv run --project harness nslab op redis get_by_key --params '{"id":42}'    # ad-hoc single operation
uv run --project harness nslab info mongodb --target replica               # version / role / replication status of a target
uv run --project harness nslab phase redis backup                          # backup -> mutate -> restore -> verify on a loaded, running stack
uv run --project harness nslab backup mongodb                              # ad-hoc backup (prints tag), exported to data/backups/mongodb/<tag>/
uv run --project harness nslab backup mongodb --restore --tag <tag> --import  # copy the export back into the (fresh) stack and restore it
uv run --project harness nslab dr mongodb                                  # DR drill: backup -> export -> down -v -> fresh up -> import -> restore -> verify
uv run --project harness nslab slowlog neo4j --exercise --threshold-ms 100  # provoke a slow query, read the engine's slow log back
uv run --project harness nslab down cassandra                              # stop + remove volumes
```
Backups are taken into the stack's `backups` volume (`/backups` inside the containers; `data/<engine>-backups/` for the
embedded engines) — which `down -v` removes — so `nslab backup` copies the artifact out to `data/backups/<stack>/<tag>/`;
keep that directory somewhere safe. The harness's own run log is `results/logs/nslab.jsonl` (rotated JSON lines with
`run_id`/`stack`/`phase`; `NSLAB_LOG_LEVEL=DEBUG` for more).

## 5. Run everything
```bash
scripts/run-all.sh --failover redis mongodb cassandra neo4j neo4j-cluster elasticsearch lmdb rocksdb
```
Sequential on purpose: two benchmarks on one host distort each other. Per-stack logs land in `results/logs/<stack>.log`.
With no stack list it runs every configured stack.

## 6. Read the results
```bash
uv run --project harness nslab report                        # rebuilds results/SUMMARY.md + results/summary.json
less results/SUMMARY.md                                      # engines, operation matrix, feature probes, optimisation, connections, replication, load test, backup & restore, logging
python3 -m json.tool results/redis/latest.json | less        # raw numbers, checksums, errors for one engine
uv run --project harness python scripts/build-report.py      # regenerates docs/report.html (self-contained interactive page)
```
Reading a result: operation `status: ok` means it ran and its result checksum is recorded; `unsupported` means the engine
has no way to express it; `error` means it was attempted but failed at run time (error text stored). `docs/findings.md`
is the written interpretation; `docs/engine-matrix.md` lists images/versions/drivers and the portability traps.

## 7. Use the skills
`skills/` holds ten first-party Agent Skills (SKILL.md + references + scripts) and vendored third-party ones. Install by
copying/symlinking a directory into `~/.claude/skills/` or `.claude/skills/`. The scripts also work standalone:
```bash
uv run --project harness python skills/nosql-benchmark-timing/scripts/bench_op.py redis get_by_key --iters 50
uv run --project harness python skills/nosql-replication-failover/scripts/lag_probe.py redis --replica replica
uv run --project harness python skills/nosql-backup-restore/scripts/backup_drill.py mongodb        # backup -> mutate -> restore -> verify
uv run --project harness python skills/nosql-logging-observability/scripts/slowlog.py redis --exercise
```

## 7b. Run a scenario (domain workload)
Beyond the per-operation catalog, a **scenario** is a realistic domain workload with a seeded corpus and a weighted,
hotspot-skewed, multi-process load. The first is **"Chatter"** (a social messaging store), built for Cassandra:
```bash
uv run --project harness nslab up cassandra
uv run --project harness nslab scenario seed  cassandra --size small          # smoke|small|full: social graph + message backlog
uv run --project harness nslab scenario run   cassandra --size small --seconds 25 --workers 8,32,64,128 --fanout write
uv run --project harness nslab scenario run   cassandra --size small --fanout read    # fan-in-on-read comparison
uv run --project harness nslab scenario curves cassandra --size small                 # send-vs-group-size, pymk-vs-degree
uv run --project harness nslab scenario run   cassandra --rate 5000 --workers 64       # open-loop at a target ops/s
uv run --project harness nslab scenario drop  cassandra
```
Results land in `results/cassandra/scenario-messaging-*.json`; `scripts/build-report.py` renders them. Design and data
model: `docs/scenario-messaging.md`. Add a scenario under `harness/nslab/scenarios/` (define the op set + backend
contract engine-agnostically, implement a backend per engine).

## 7c. Run it on Kubernetes (k3s via k3d + a local Harbor registry)
```bash
cp k8s/.env.example k8s/.env && chmod 600 k8s/.env   # HARBOR_HOST = a LAN IP; reuse the rd-databases Harbor or let up.sh install one
make -C k8s harbor                                   # project nslab + push/pull robot accounts (secrets recorded in k8s/.env)
make -C k8s push                                     # mirror the 8 images into Harbor (k8s/images.lock.json records digests)
make -C k8s gen up                                   # generate stacks/*/k8s/, create the k3d cluster "nslab", smoke-test a pull
NSLAB_PLATFORM=k3s uv run --project harness nslab run redis --keep --failover        # same phases, kubectl underneath
NSLAB_PLATFORM=k3s uv run --project harness nslab dr redis                           # the DR drill: namespace deleted, fresh apply, restore
NSLAB_PLATFORM=k3s uv run --project harness nslab scenario seed neo4j --scenario recgraph --size small
k8s/scenarios.sh status redis | failover redis | chaos mongodb | teardown redis
k8s/run-all-k3s.sh                                   # every stack: all phases + failover + DR drill, sequentially
make -C k8s down                                     # delete the cluster (Harbor keeps running)
```
Results land in `results/k3s/<engine>/` and appear as `<engine> (k3s)` rows in `results/SUMMARY.md` and the report.
Details, the `x-k8s` compose hints and what is not emulated: **[docs/kubernetes.md](docs/kubernetes.md)**.

## 8. Add your own operation, probe, engine or stack
- **Operation**: append an `Op(...)` to `harness/nslab/ops.py` (id, kind, a param generator, a contract string), then
  implement `op_<id>(self, params)` on the engine adapters that can express it. Missing method = reported `unsupported`.
- **Probe**: add a `Probe(...)` to `harness/nslab/probes.py` and a `probe_<name>(self)` method (or `STATIC_PROBES` entry).
- **Engine**: register the driver in `harness/nslab/engines/__init__.py`, write `engines/<mod>.py` (subclass `Engine` +
  `Client`, implement the primitives and `op_*`/`probe_*`), and add `stacks/<key>/compose.yaml` + `lab.yaml` (copy the
  closest one; `family`, `driver`, `targets`, `replication`, `features`). Reuse an existing adapter via `features` when the
  engine speaks a known protocol (RESP, MongoDB wire, CQL, Bolt, ES REST).

## 9. Clean up
```bash
uv run --project harness nslab down <stack>       # one stack, removes its volumes
docker ps -a --filter name=nslab- -q | xargs -r docker rm -f
docker volume ls -q --filter name=nslab | xargs -r docker volume rm
rm -rf data/ results/logs/                        # dataset and logs (results/*/latest.json are the deliverables, keep them)
```

## Troubleshooting
| symptom | cause / fix |
|---|---|
| `toomanyrequests` when pulling | Docker Hub anonymous limit: use `scripts/pull-images.sh` (mirror) or `docker login` |
| MongoDB container exits instantly | `mongo:8` crashes on Linux ≥6.19/7.0 (SERVER-121912); the lab pins `mongo:7`. On a newer kernel use MongoDB ≥8.0.30/8.3.9-patched |
| Elasticsearch exits at startup | needs `memlock -1` + `nofile 65536` ulimits and enough heap; disk-watermark disabled on a full host |
| Cassandra ring splits / node stays DN | give `start_period`, join nodes sequentially, `memlock: -1`; first node up takes ~60-75 s |
| Cassandra `ReadFailure ... tombstones` | a bulk load bound `NULL` instead of `UNSET_VALUE`; scans then choke on tombstones (fixed in the adapter) |
| embedded engine "environment already open" | LMDB/RocksDB allow one open per process; the harness shares one handle (don't open a second) |
| numbers look slow / noisy | something else is running on the host; run one stack at a time (`scripts/run-all.sh`) and check `docker stats` |
| failover run finished but the stack is "broken" | expected: the old primary is left out; `nslab down <stack>` and start fresh |
| backup phase: `restore ... verified=False` | the pre-backup fingerprint was already mutated (a previous run failed mid-way): re-run with `--phases load,backup` |
| ES snapshot repository "cannot create blob store" | `/backups` must be writable by uid 1000: the phase does `chmod 1777 /backups`; do the same after a manual `up` |
| Neo4j `neo4j-admin database dump` "is not an existing directory" | create `/backups/<tag>` first (writable by the `neo4j` user); the phase does |
