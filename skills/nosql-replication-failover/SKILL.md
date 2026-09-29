---
name: nosql-replication-failover
description: Set up and reason about NoSQL replication and failover - async primary/replica with Sentinel (Redis), replica sets with automatic elections (MongoDB), leaderless quorum replication (Cassandra/Dynamo-style), single-primary graph clustering (Neo4j Enterprise), and search-engine primary/replica shards (Elasticsearch). Covers replica read routing and write rejection, visibility/replication lag, catch-up under load, read scaling, and measuring failover downtime by killing the primary. Use when choosing a topology, configuring replicas/quorum, routing reads, or planning for node failure.
---

# NoSQL replication & failover

## The four topologies (and who uses them)
- **Async primary/replica + external failover** — *Redis* (+ Sentinel or Cluster). One writable primary, read-only
  replicas, async propagation. A separate quorum (3 Sentinels) detects primary death and promotes a replica.
- **Replica set with built-in election** — *MongoDB*. Members vote; the primary takes writes, secondaries replicate the
  oplog and reject writes; on primary loss the members elect a new one automatically.
- **Leaderless quorum (Dynamo-style)** — *Cassandra/ScyllaDB/Dynamo*. No primary: every node accepts writes, RF copies,
  read/write consistency chosen per request (R+W>RF for strong). Node death needs no promotion — remaining replicas serve.
- **Single-primary cluster** — *Neo4j causal cluster* (Enterprise, `stacks/neo4j-cluster`), *Elasticsearch* (primary +
  replica shards per index, `stacks/elasticsearch` = 3 nodes). ES routes reads to any shard copy and re-allocates shards
  on node loss; Neo4j routes writes to the leader, reads to followers (`neo4j://` routing).

## What the lab measures (replication phase)
1. **Topology** — role each node reports.
2. **Visibility lag** — write on primary, poll a replica until the key appears. Redis p50 **0.74 ms**, MongoDB **1.35 ms**
   (async oplog / diskless replication over localhost).
3. **Replica write rejection** — Redis replica → `READONLY`; MongoDB secondary → `NotWritablePrimary` (10107);
   Cassandra node → **accepts** (leaderless, no primary). This tells you whether you can route writes anywhere.
4. **Catch-up under load** — 20k writes, then time each replica to converge.
5. **Read scaling** — point-read throughput on primary-only vs primary+replicas. On a single host adding contended
   replicas did not help (Redis ×1.02, MongoDB ×1.01, Cassandra ×0.73) — read scaling shows up across *machines*, not
   cores on one box; measure it on real nodes.
6. **Failover** (`--failover`, destructive): kill the primary container — for elected clusters the member that
   *currently* holds the role (`engine.leader_target`: ES master, Neo4j Raft leader) — (promote), poll until a write
   succeeds again through a surviving entry point. **Downtime measured**: Redis Sentinel **~2.5 s** (`down-after 2 s` +
   election), Cassandra leaderless **~0.9 s** (no election — write goes to another node), Elasticsearch master kill
   **4.9 s** (master election + primary-shard promotion, cluster yellow until the node returns), Neo4j Enterprise Raft
   leader kill **20.9 s** (election + the driver's routing-table refresh), MongoDB replica set **~30 s** here (default
   `electionTimeoutMillis` plus the driver's server-selection retry loop; tune `settings.electionTimeoutMillis`, member
   priorities and client `serverSelectionTimeoutMS` to bring it down to seconds).

## Configuration that matters
- **Redis**: `replicaof`, `repl-diskless-sync yes`, AOF `everysec` on both; Sentinel `monitor <name> <host> <port>
  <quorum>`, `down-after-milliseconds`, `failover-timeout`; clients use a Sentinel-aware pool so they follow the promotion.
- **MongoDB**: `rs.initiate({members:[{priority}...]})`; write concern `majority` for durability across the election;
  clients connect with `replicaSet=` (not a fixed host) so the driver finds the new primary.
- **Cassandra**: keyspace `NetworkTopologyStrategy` RF=3, read/write at `LOCAL_QUORUM`; no promotion — size for N-1 nodes.
- **Elasticsearch**: `number_of_replicas ≥ 1` on a multi-node cluster (single node = 0, no HA); shards re-allocate on loss.
  The lab runs 3 nodes with `number_of_replicas 2`: every node holds every shard, any node coordinates writes (no
  write rejection anywhere), replica GET lag ~5 ms, and writes pay for the copies (insert 5.2 → 7.1 ms, bulk 217k → 112k
  docs/s). An `fs` snapshot repository must be one shared path on every node.
- **Neo4j Enterprise cluster**: `initial.dbms.default_primaries_count=3` (Raft; ≥3 for automatic failover), clients use
  `neo4j://` routing so writes always reach the leader — and `server.bolt.advertised_address` must be reachable from
  the client (the lab advertises host-mapped ports). Followers reject direct writes (`WriteServiceUnavailable` on a
  `bolt://` connection); follower lag ~7 ms; every commit is a Raft round (insert 5.9 → 10.3 ms vs Community).
  Community cannot cluster at all.

## Routing reads
- Redis: connect to a replica (or Sentinel `slave_for`) for read-only, stale-tolerant traffic; keep writes on the primary.
- MongoDB: `readPreference=secondaryPreferred`/`nearest` for offload, `primary` for read-your-writes; combine with
  `readConcern` for freshness guarantees.
- Cassandra: the driver load-balances; consistency level controls how many replicas answer.
- ES: reads already hit any copy; `?preference=` pins a replica for cache locality.
Never route writes to a replica/secondary — the lab shows they reject (Redis/MongoDB) or you rely on leaderless behaviour
(Cassandra) where you must still meet write consistency.

## Files
- `scripts/lag_probe.py` — write on the primary, measure visibility lag on a replica of a running stack.
