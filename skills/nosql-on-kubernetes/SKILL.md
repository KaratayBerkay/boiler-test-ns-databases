---
name: nosql-on-kubernetes
description: Run NoSQL database stacks (Redis+Sentinel, MongoDB replica set, Cassandra ring, Neo4j single/Raft cluster, Elasticsearch cluster) on k3s via k3d with a local Harbor registry, generating Kubernetes manifests from Docker Compose files, and drive the same benchmark/backup/failover harness against the cluster. Covers the k3d + Harbor + robot-account boilerplate, compose->kustomize generation (Deployments, headless Services, NodePorts, PVCs, Jobs), the NoSQL-specific translation traps (static IPs, advertised Bolt addresses, memlock, JMX for nodetool, RWO PVCs shared by pods, exec users), CronJob backups into a shared PVC, and what changes between compose and k3s numbers. Use when asked to put a database lab on Kubernetes, build a private registry boilerplate, write StatefulSet/Deployment manifests for NoSQL engines, or make a compose-based harness also run against kubectl.
---

# NoSQL stacks on k3s (k3d + Harbor + generated manifests)

The lab's second runtime, mirrored from `rd-databases/k8s`: one Harbor per host with a project per lab, one k3d
cluster per lab, manifests generated from the compose files, and a `NSLAB_PLATFORM=k3s` shim that routes the harness's
container primitives (exec, logs, stop/start, cp, helper containers, up/down) to `kubectl`. Files: `k8s/` (Makefile,
`harbor/up.sh` + `setup.py`, `push-images.sh`, `gen.py`, `up.sh`, `scenarios.sh`, `scenarios-backup.sh`,
`gen-backup-addons.py`, `run-all-k3s.sh`), `harness/nslab/platform.py`, `docs/kubernetes.md`.

## The boilerplate, in order
1. **Harbor** (`k8s/harbor/up.sh`): official online installer via docker compose, self-signed CA + cert with the LAN IP
   as hostname (host *and* nodes must reach the same address — Harbor puts it in the token realm), a private project
   per lab, two project-scoped robots (`+pusher` push/pull for the host, `+k3s` pull-only for nodes). Reuse an
   instance that already answers; copy its CA. Never push as admin.
2. **Mirror images** (`k8s/push-images.sh`): `docker tag` + `docker push` through `127.0.0.1:<port>` (loopback is an
   insecure registry for Docker, no daemon config), `crane copy` as fallback when the daemon refuses to push an image it
   pulled ("does not provide any platform"), digests locked in `images.lock.json`.
3. **Generate** (`k8s/gen.py`): `docker compose config` → Deployment (1 replica, Recreate) + headless Service per
   service (`publishNotReadyAddresses: true` so bootstrap traffic flows before readiness) + NodePort Service per
   published port (stable host-port → nodePort table, published on `127.0.0.1:<nodePort>` by k3d) + PVC per named
   volume + ConfigMap per bind mount + Job/initContainer per one-shot + readiness/startup probes from healthchecks
   (no liveness: failover tests stop primaries on purpose).
4. **Cluster** (`k8s/up.sh`): `k3d cluster create --config k3d.yaml --registry-config registries.yaml` (mirror +
   robot auth + CA file mounted into the node), traefik off, kubelet log rotation 50 Mi × 3, nodePort range 30000-32767;
   smoke test = a pod pulling busybox from Harbor with the pull-only robot.
5. **Drive**: `NSLAB_PLATFORM=k3s nslab run <stack> --keep --failover`, `nslab dr <stack>`, `nslab scenario …`;
   `k8s/scenarios.sh deploy|failover|chaos|rolling`, `k8s/scenarios-backup.sh schedule|backup-now|restore-drill`.

## What NoSQL engines need that a generic compose→k8s translation does not give you
- **Static container IPs are gone.** Cassandra's compose ring pins nodes to `10.99.1.x` and seeds by IP; on k8s seed by
  the headless-Service name (`CASSANDRA_SEEDS: cassandra1`), gossip on pod IPs, and publish each node's 9042 as its own
  NodePort so a client can still pin to one node (`x-k8s: {publish: {"9042": 19042}}` + `k8s_targets:` in lab.yaml).
- **Advertised addresses must be host-reachable.** A Neo4j `neo4j://` routing client on the host gets the cluster's
  `server.bolt.advertised_address` list; advertise `127.0.0.1:<nodePort>` (generator substitutes `$${NODEPORT:17687}`)
  or every routed write fails with "Unable to retrieve routing information".
- **No ulimits.** `bootstrap.memory_lock=true` makes Elasticsearch's bootstrap check refuse to start in production mode
  (multi-node); set it false on k8s. Cassandra/ES `memlock`/`nofile` become node defaults — warn, don't fail.
- **nodetool over JMX.** A backup CronJob cannot `docker exec`; open JMX in-namespace (`LOCAL_JMX=no` +
  `JVM_EXTRA_OPTS=-Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false`) and run
  `nodetool -h cassandraN snapshot`; export the snapshot from the node's data PVC (mount it read-only in the job).
- **Shared RWO volume.** The stack's `backups` volume becomes one PVC mounted by every pod of the stack and by helper
  pods — fine on a single node (local-path), a multi-node cluster needs RWX (NFS) or per-node exports.
- **Stopping a member = scaling its Deployment to 0.** Sentinel, replica-set and Raft members see a dead peer exactly
  as with `docker kill`; a Deployment at 0 has no pod, so resolve the Deployment by label (not the pod) to start it again.
- **exec user.** `kubectl exec` has no `-u`; the official images run root at the entrypoint and drop privileges, so
  root-level file operations in the pod work, but Elasticsearch runs as uid 1000 — put anything it must write on a
  world-writable path (the harness `chmod 1777 /backups` once per stack).
- **Two clusters on one host** must not share nodePort→loopback mappings: the rd lab publishes 30000-30069, this one
  31000+ (`NODEPORT_BASE`).

## What changes in the numbers (compose → k3s, same host)
- Point reads/writes pay ~0.3 ms for the NodePort hop (Redis GET 0.21 → 0.50 ms, MongoDB 0.79 → 1.08 ms); bulk
  loads and throughput are within noise (Redis 89.9k → 89.6k docs/s, ES 112k → 120k, Cassandra 33.7k → 28.3k).
- Backups: same times (they run inside the pods); restores that restart a member are slower because "start" means
  scheduling a new pod behind a readiness probe (Redis 24 s → 84 s). All restores and DR drills verified on all six stacks.
- Failover with real SIGKILL semantics (`kill -9 1` in the pod, then scale to 0): MongoDB 0.9 s (vs 30 s compose — the
  pod's network namespace vanishes, peers see a closed connection instead of a heartbeat timeout), Neo4j Raft 11 s
  (vs 21 s), ES master 1.0 s (vs 4.9 s), Redis Sentinel 3.2 s, Cassandra 1.6 s. A plain scale-down is a graceful
  SIGTERM (MongoDB steps down, Neo4j hands over the lead) — not a crash test.
- Scenarios run unchanged: Chatter on Cassandra 2.9k ops/s at 64 workers, GraphRec on Neo4j 0.9k ops/s at 32.

## Scheduled backups (CronJobs)
`k8s/gen-backup-addons.py` writes one CronJob per stack into the stack's namespace: redis `redis-cli --rdb` (SYNC
stream, no server volume needed), mongodump `--oplog`, Cassandra `nodetool snapshot` + export from the data PVCs,
Neo4j Enterprise `neo4j-admin database backup --from=<member>:6362`, Elasticsearch `_snapshot` + retention through the
API. Community Neo4j cannot be backed up online — the harness stops the pod (`nslab phase neo4j backup`). Retention =
keep the newest 5 under `/backups/cron/`. `k8s/scenarios-backup.sh backup-now <stack>` runs one immediately.

## Related
- `references/generator-notes.md` — how `gen.py` maps each compose construct, the `x-k8s` hints, and the platform shim.
- Sibling skills: `docker-compose-nosql` (the source of truth the manifests are generated from), `nosql-backup-restore`
  (what the CronJobs and drills exercise), `nosql-replication-failover` (what "scale to 0" measures).
