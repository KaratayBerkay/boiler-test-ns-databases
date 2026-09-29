# Running the lab on k3s (Kubernetes) with a local Harbor registry

The lab was built as Docker Compose stacks. This adds a **second runtime**: the same stacks, the same seeded dataset,
the same harness phases *and the same domain scenarios*, running on a **k3s cluster** (via k3d) that pulls every image
from a **local Harbor registry**. Compose stays the source of truth — the Kubernetes manifests are generated from
`stacks/<engine>/compose.yaml`, so a change to a compose file flows into both runtimes. It is the same layout as the
sibling `rd-databases/k8s` (one Harbor per host, one project per lab, one k3d cluster per lab), so the two labs are a
matching pair of boilerplates.

```
stacks/<engine>/compose.yaml ──(k8s/gen.py)──▶ stacks/<engine>/k8s/*.yaml ──(kubectl apply -k)──▶ k3d cluster "nslab"
        │                                                                                              │
        └── images ──(k8s/push-images.sh)──▶ Harbor 10.10.2.51:8443/nslab/…  ──(pull-only robot)──────┘
```

## Why this shape

| decision | reason |
|---|---|
| **k3d** (k3s in Docker), not the host k3s service | no root needed, disposable, reproducible from one config file; the rd-databases lab on this host runs the same way |
| **one shared Harbor**, projects `rdlab` and `nslab` | one private mirror per host: no Docker Hub rate limits, one download of each image, project-scoped robot accounts (`robot$nslab+pusher` push/pull from the host, `robot$nslab+k3s` pull-only for the nodes) |
| **one cluster per lab**, nodePorts 31000+ | the rd cluster publishes 30000-30069 on the same loopback; separate clusters keep resource use and failures apart |
| **manifests generated from compose** | 6 stacks / 20 services; `docker compose config` resolves anchors and env, so what runs on k8s is what the compose file says. NoSQL-specific deltas are `x-k8s` hints in the compose files, not hand-edited YAML |
| **NodePort per published port**, published on `127.0.0.1:<nodePort>` | `lab.yaml` targets work unchanged: `NSLAB_PLATFORM=k3s` maps the compose host port through `k8s/ports.json` |
| **Deployment + headless Service**, `publishNotReadyAddresses: true` | cluster bootstrap traffic (Sentinel reaching `primary`, Mongo `rs.initiate` on `mongo1:27017`, Cassandra gossip to `cassandra1`, ES discovery, Neo4j Raft) must flow before readiness, exactly like a compose network |
| **no livenessProbe** | the failover phase stops a primary on purpose; a liveness probe would fight it |
| **one node, RWO PVCs** | the shared `backups` volume of every stack is one PVC mounted by all of its pods — fine on one node, and the backup phase's helper pods mount the same claim |

## NoSQL-specific translation (`x-k8s` hints in the compose files)

| stack | what differs from compose | hint |
|---|---|---|
| Cassandra | nodes addressed by static container IPs (`10.99.1.11-13`), no published ports | `x-k8s: {port: 9042, publish: {"9042": 19042}, env: {CASSANDRA_SEEDS: cassandra1}}` per node → gossip by headless-Service name, one NodePort per node; `lab.yaml` carries `k8s_targets:` (127.0.0.1 + the pseudo host ports 19042-19044) |
| Neo4j Enterprise cluster | `server.bolt.advertised_address=127.0.0.1:17687` (compose host ports) — the routing table must name ports the host can reach | `x-k8s: {env: {NEO4J_server_bolt_advertised__address: "127.0.0.1:$${NODEPORT:17687}"}}` — the generator substitutes the assigned nodePort |
| Elasticsearch | `bootstrap.memory_lock=true` needs an unlimited memlock ulimit; Kubernetes cannot grant it and the bootstrap check refuses to start | `x-k8s: {env: {bootstrap.memory_lock: "false"}}` |
| Redis / MongoDB / Neo4j CE | nothing — Sentinel, HAProxy and `rs-init.js` already use service names (`primary`, `replica`, `mongo1`) | — |

`ulimits` (nofile/memlock) are not representable and are reported as warnings by `gen.py`; node defaults apply.

## One-time setup

```bash
cd ns-databases
cp k8s/.env.example k8s/.env && chmod 600 k8s/.env   # HARBOR_HOST = a LAN IP both host and nodes reach; same admin password as rd-databases when sharing its Harbor
k8s/harbor/up.sh          # reuses a Harbor already answering at HARBOR_HOST:8443 (copies its CA) or installs one; creates project nslab + 2 robots
k8s/push-images.sh        # mirrors the 8 images (redis, mongo, cassandra, neo4j community+enterprise, elasticsearch, haproxy, busybox); writes k8s/images.lock.json
make -C k8s gen           # stacks/*/k8s/ + k8s/ports.json + k8s/k3d.yaml + k8s/registries.yaml
k8s/up.sh                 # k3d cluster "nslab" wired to Harbor; smoke-tests a pull with the pull-only robot
```

`k8s/.env` holds the admin password and the two robot secrets (Harbor shows a robot secret once; `setup.py` records
it). It is gitignored, mode 600. So are `k8s/harbor/{certs,data,logs,dist}`, `k8s/.docker-robot`, `k8s/registries.yaml`.

## Deploy and drive a stack

```bash
kubectl config use-context k3d-nslab
kubectl apply -k stacks/mongodb/k8s && kubectl -n nslab-mongodb get pods,svc,pvc

# the harness, against the cluster: NSLAB_PLATFORM=k3s routes exec/logs/lifecycle/cp to kubectl and remaps ports
NSLAB_PLATFORM=k3s uv run --project harness nslab run mongodb --keep --failover        # up = kubectl apply -k, all nine phases
NSLAB_PLATFORM=k3s uv run --project harness nslab dr mongodb                           # backup -> export -> delete namespace -> fresh apply -> import -> restore -> verify
NSLAB_PLATFORM=k3s uv run --project harness nslab scenario seed cassandra --size small # the domain scenarios, unchanged
make -C k8s run STACK=redis PHASES=load,bench                                           # shorthands
make -C k8s scenario STACK=neo4j NAME=recgraph SIZE=small
k8s/scenarios.sh deploy redis | status redis | failover redis | chaos cassandra | teardown redis   # kubectl-only stories
```

Results of k3s runs land in **`results/k3s/<engine>/`** (same layout as the compose results) and appear in
`results/SUMMARY.md` / the report as `<engine> (k3s)` rows next to the compose numbers.

`NSLAB_PLATFORM=k3s` (see `harness/nslab/platform.py`) is the whole switch:

- container ops (`exec_in`, `logs`, `du`, `inspect`, stop/start/kill, `docker cp`, one-shot helper containers, up/down)
  resolve the compose container name to a pod through the `nslab.io/container` label and run `kubectl` instead of
  `docker`: stop/kill = scale the owning Deployment to 0 (the pod is gone, Sentinel/replica-set/Raft see a dead
  member), start = scale back to 1, a helper container = a throw-away Pod mounting the same PVCs;
- `lab.yaml` target ports are translated to their nodePort via `k8s/ports.json` (`k8s_targets:` overrides first).

Not emulated, reported honestly: per-container CPU/memory in the load test (host cgroups are invisible inside the k3d
node; docs/s is still measured) and docker `pause`.

## Files

| path | what |
|---|---|
| `k8s/harbor/up.sh`, `setup.py` | reuse/install Harbor; create the `nslab` project + `pusher`/`k3s` robots |
| `k8s/push-images.sh` | mirror every stack image into Harbor (crane fallback), lock digests in `k8s/images.lock.json` |
| `k8s/gen.py` | compose → k8s generator: `images`, `ports`, `k3d`, `manifests`; `x-k8s` hints (`port`, `publish`, `env` with `$${NODEPORT:n}`) |
| `k8s/up.sh` / `down.sh` | create/delete the k3d cluster (`k8s/k3d.yaml` + `k8s/registries.yaml`) |
| `k8s/scenarios.sh` | deploy / status / failover / scale-reads / rolling / chaos / teardown / verify-all / `lab <stack> …` |
| `k8s/Makefile` | `env harbor push gen up deploy status failover run scenario down` |
| `k8s/gen-backup-addons.py`, `k8s/addons/backup/<stack>/` | CronJob per stack (scheduled backups into the backups PVC, retention 5) |
| `k8s/scenarios-backup.sh` | `schedule / backup-now / artifacts / restore-drill / dr-drill / logging-drill / promote` |
| `k8s/run-all-k3s.sh` | every stack on the cluster: all phases + failover + DR drill, sequentially |
| `k8s/ports.json` | host port → nodePort (31000+); recreate the cluster after it changes |
| `stacks/<engine>/k8s/` | generated Namespace, PVCs, ConfigMaps, Services, Deployments/Jobs, kustomization |
| `harness/nslab/platform.py` | the `NSLAB_PLATFORM=k3s` shim dockerctl and config delegate to |
| `results/k3s/<engine>/` | results of k3s runs (`latest.json`, scenario files) |

## Scheduled backups on the cluster (CronJob add-ons)

The compose-side backup strategy says "schedule: cron"; on Kubernetes that is a CronJob per stack, generated by
`k8s/gen-backup-addons.py` into `k8s/addons/backup/<stack>/` (separate kustomize dirs in the stack's namespace, so
regenerating the stack manifests never touches them). Each runs the engine's own image from Harbor, connects through
the primary's Service name, writes `/backups/cron/<timestamp>*` into the stack's shared `backups` PVC — the same volume
the harness's backup phase and DR drill use — and keeps the newest 5.

| stack | image | what the CronJob runs |
|---|---|---|
| redis | redis:8 | `redis-cli -h primary --rdb /backups/cron/<ts>.rdb` (SYNC stream written by the client — works with an AOF-only primary, no access to the server's volume needed) |
| mongodb | mongo:7 | `mongodump --host=mongo1:27017 --oplog --gzip --archive=/backups/cron/<ts>.archive.gz` |
| cassandra | cassandra:5 | `nodetool -h cassandraN flush + snapshot -t cron-<ts> --ttl 24h lab` on every node (JMX opened in-namespace by an `x-k8s` env hint), then every node's snapshot exported from its data PVC into `/backups/cron/<ts>/nodeN/lab/<table>/` |
| neo4j-cluster | neo4j:5-enterprise (uid 7474) | `neo4j-admin database backup lab --from=neo4j1:6362 --type=full` (online, over the backup port) |
| elasticsearch | elasticsearch:9.5.3 (uid 1000) | `PUT _snapshot/nslab/cron-<ts>?wait_for_completion=true` + delete the oldest beyond 5 (repository on the shared volume) |
| neo4j (Community) | — | no online backup: the harness stops the server for `neo4j-admin database dump` (`nslab phase neo4j backup`) |

```bash
uv run --project harness python k8s/gen-backup-addons.py     # (re)generate k8s/addons/backup/*
k8s/scenarios-backup.sh schedule redis mongodb               # CronJobs backup-<stack> in nslab-<stack>, "0 */6 * * *"
k8s/scenarios-backup.sh backup-now mongodb                   # kubectl create job --from=cronjob/backup-mongodb, wait, logs
k8s/scenarios-backup.sh artifacts mongodb                    # ls -la /backups/cron in the primary pod
k8s/scenarios-backup.sh restore-drill redis                  # = NSLAB_PLATFORM=k3s nslab phase redis backup
k8s/scenarios-backup.sh dr-drill redis                       # = NSLAB_PLATFORM=k3s nslab dr redis
k8s/scenarios-backup.sh promote neo4j-cluster                # = NSLAB_PLATFORM=k3s nslab run neo4j-cluster --failover --phases replication
```

## What was run on k3s (this host, 2026-09-14)

| engine | load events/s k3s / compose | get p50 ms | insert p50 ms | bulk docs/s | backup s / restore s, verified | DR drill | failover ms k3s / compose | slow log |
|---|---|---|---|---|---|---|---|---|
| Cassandra 5.0 | 17806 / 17454 | 2.58 / 1.77 | 1.14 / 1.25 | 28267 / 33667 | 11.72 s / 27.80 s, ✅ | ✅ | 1612 / 900 | ✅ |
| Elasticsearch 9 | 33762 / 29495 | 4.33 / 3.92 | 7.42 / 7.07 | 120200 / 111533 | 7.74 s / 10.39 s, ✅ | ✅ | 1046 / 4870 | ✅ |
| MongoDB 7.0 | 46916 / 46677 | 1.08 / 0.79 | 4.00 / 3.96 | 138267 / 156000 | 15.86 s / 76.00 s, ✅ | ✅ | 923 / 30504 | ✅ |
| Neo4j 5 Enterprise cluster | 8008 / 7314 | 3.62 / 2.63 | 8.28 / 10.25 | 12467 / 11067 | 25.00 s / 16.41 s, ✅ | ✅ | 11257 / 20900 | ✅ |
| Neo4j 5 | 7281 / 9248 | 2.82 / 2.65 | 5.28 / 5.92 | 14200 / 14600 | 53.24 s / 63.56 s, ✅ | ✅ | - / - | ✅ |
| Redis 8 | 41993 / 41922 | 0.50 / 0.21 | 0.97 / 0.30 | 89600 / 89867 | 7.26 s / 84.29 s, ✅ | ✅ | 3181 / 2500 | ✅ |

Same host, same dataset, same harness; "k3s / compose" pairs are the k3s number first. Point operations pay for the
NodePort hop (kube-proxy → pod: Redis GET 0.50 vs 0.21 ms, MongoDB 1.08 vs 0.79 ms), bulk throughput and load rates
are within noise. Backups and restores go through the same engine tooling (helper Pods over the PVCs instead of
helper containers); the Redis restore is slower on k3s (84 s vs 24 s) because scaling the Deployment back to 1
schedules a new pod and the RDB load + replica full-resync run behind a readiness probe. Every restore and every DR
drill (namespace deleted, manifests re-applied, artifact imported with `kubectl cp`) verified on all six stacks.

**Failover on k3s** = `kill -9 1` inside the pod, then the Deployment scaled to 0 (SIGKILL semantics, like `docker
kill`; a plain scale-down would be a graceful SIGTERM in which MongoDB steps down and Neo4j hands over the Raft lead —
much faster, and not a crash). Pod deletion tears down the network namespace immediately, so peers see a closed
connection rather than a silent 10 s heartbeat timeout: MongoDB elects in 0.9 s here vs 30 s under compose, Neo4j Raft
11 s vs 21 s, Elasticsearch 1.0 s vs 4.9 s; Redis Sentinel (3.2 s) and Cassandra (1.6 s) are unchanged in kind.

**Scenarios on k3s** (`NSLAB_PLATFORM=k3s nslab scenario …`, results in `results/k3s/<engine>/scenario-*`):
Chatter on Cassandra (size small: 40k users, 60k conversations, 8M messages seeded at 15.5k msgs/s) peaked at
2,936 ops/s with 64 workers (compose: 637 at the same ramp in this session's serial re-run; the earlier compose figure
of 2,064 came from a different worker ramp); GraphRec on Neo4j peaked at 922 ops/s with 32 workers (compose 756).

**CronJob backups** (`k8s/scenarios-backup.sh backup-now <stack>`): Redis `redis-cli --rdb` 336 MB, MongoDB
`mongodump --oplog` 51 MB, Cassandra `nodetool snapshot` over JMX on all three nodes + export 363 MB — all three
verified against loaded stacks; Neo4j Enterprise and Elasticsearch CronJobs are generated from the same recipes the
harness's backup phase runs (`neo4j-admin database backup`, `_snapshot`) and exercised by that phase.


## Regenerating after a compose change

```bash
make -C k8s gen                     # re-render manifests + ports + k3d config
k8s/up.sh --recreate                # only if ports.json changed (port mappings are fixed at cluster creation)
kubectl apply -k stacks/<engine>/k8s
```
