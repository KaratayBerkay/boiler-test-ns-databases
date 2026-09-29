# Replication & failover

Visibility lag = p50 time from write on the primary to read-your-write on a replica. "Replica rejects writes" is
what happens when a client writes to a non-primary. Catch-up = ms for a replica to drain a 20k-doc burst.
Read-scaling gain = throughput multiplier from adding read copies. Failover downtime = ms until a write succeeded
again after killing the primary/leader/master.

| engine | kind | visibility lag p50 | replica rejects writes | catch-up 20k docs | read-scaling gain | failover downtime |
|---|---|---:|---|---:|---:|---:|
| MongoDB 7.0 | replica set (oplog, async, automatic election) | 1.35 ms | yes — | 29 ms | ×1.01 | 30,504 ms |
| MongoDB 7.0 (k3s) | replica set (oplog, async, automatic election) | 1.29 ms | yes — | 28 ms | ×1.01 | 923 ms |
| LMDB | none | single node | — | — | — | — |
| RocksDB | none | single node | — | — | — | — |
| Neo4j 5 | none (single instance; Enterprise causal clustering not in Community) | single node | — | — | — | — |
| Neo4j 5 (k3s) | none (single instance; Enterprise causal clustering not in Community) | single node | — | — | — | — |
| Neo4j 5 Enterprise cluster | Raft cluster of 3 primaries (leader takes writes, followers replicate; automatic leader election) | 8.04 ms | yes — | 18 ms | ×1.24 | 20,900 ms |
| Neo4j 5 Enterprise cluster (k3s) | Raft cluster of 3 primaries (leader takes writes, followers replicate; automatic leader election) | 7.25 ms | yes — | 29 ms | ×0.79 | 11,257 ms |
| Redis 8 | async primary/replica (diskless sync, AOF everysec) + Sentinel failover | 0.74 ms | yes READONLY | 1.40 ms | ×1.02 | 2,500 ms |
| Redis 8 (k3s) | async primary/replica (diskless sync, AOF everysec) + Sentinel failover | 0.79 ms | yes READONLY | 7.50 ms | ×1.01 | 3,181 ms |
| Elasticsearch 9 | primary + replica shards (number_of_replicas 2) over 3 nodes; master election by quorum | 6.42 ms | accepted | 51 ms | ×1.24 | 4,870 ms |
| Elasticsearch 9 (k3s) | primary + replica shards (number_of_replicas 2) over 3 nodes; master election by quorum | 8.88 ms | accepted | 21 ms | ×1.18 | 1,046 ms |
| Cassandra 5.0 | leaderless replication, RF=3, tunable consistency (LOCAL_QUORUM) | — | accepted | 489 ms | ×0.73 | 900 ms |
| Cassandra 5.0 (k3s) | leaderless replication, RF=3, tunable consistency (LOCAL_QUORUM) | 3.52 ms | accepted | 482 ms | ×0.82 | 1,612 ms |

## Visibility lag detail

- **MongoDB 7.0** (replica): n=30, min 0.87 ms, p50 1.35 ms, p90 1.45 ms, p95 1.47 ms, p99 1.51 ms, max 1.52 ms, mean 1.29 ms
- **MongoDB 7.0** (replica2): n=30, min 0.86 ms, p50 1.30 ms, p90 1.35 ms, p95 1.39 ms, p99 1.44 ms, max 1.46 ms, mean 1.27 ms
- **MongoDB 7.0 (k3s)** (replica): n=30, min 0.74 ms, p50 1.29 ms, p90 1.55 ms, p95 1.56 ms, p99 1.66 ms, max 1.70 ms, mean 1.29 ms
- **MongoDB 7.0 (k3s)** (replica2): n=30, min 0.91 ms, p50 1.48 ms, p90 1.58 ms, p95 1.60 ms, p99 1.63 ms, max 1.64 ms, mean 1.42 ms
- **Neo4j 5 Enterprise cluster** (node2): n=30, min 4.94 ms, p50 8.04 ms, p90 9.54 ms, p95 16 ms, p99 74 ms, max 96 ms, mean 11 ms
- **Neo4j 5 Enterprise cluster** (node3): n=30, min 4.91 ms, p50 6.97 ms, p90 10 ms, p95 18 ms, p99 79 ms, max 103 ms, mean 11 ms
- **Neo4j 5 Enterprise cluster (k3s)** (node2): n=30, min 5.07 ms, p50 7.25 ms, p90 8.21 ms, p95 14 ms, p99 46 ms, max 57 ms, mean 8.93 ms
- **Neo4j 5 Enterprise cluster (k3s)** (node3): n=30, min 4.51 ms, p50 6.85 ms, p90 11 ms, p95 22 ms, p99 65 ms, max 83 ms, mean 10 ms
- **Redis 8** (replica): n=30, min 0.33 ms, p50 0.74 ms, p90 0.87 ms, p95 0.94 ms, p99 32 ms, max 45 ms, mean 2.20 ms
- **Redis 8 (k3s)** (replica): n=30, min 0.39 ms, p50 0.79 ms, p90 0.89 ms, p95 0.94 ms, p99 2.01 ms, max 2.43 ms, mean 0.80 ms
- **Elasticsearch 9** (node2): n=30, min 3.89 ms, p50 6.42 ms, p90 15 ms, p95 20 ms, p99 25 ms, max 27 ms, mean 8.49 ms
- **Elasticsearch 9** (node3): n=30, min 3.48 ms, p50 5.11 ms, p90 6.43 ms, p95 7.32 ms, p99 15 ms, max 17 ms, mean 5.53 ms
- **Elasticsearch 9 (k3s)** (node2): n=30, min 6.00 ms, p50 8.88 ms, p90 21 ms, p95 27 ms, p99 33 ms, max 35 ms, mean 12 ms
- **Elasticsearch 9 (k3s)** (node3): n=30, min 4.54 ms, p50 7.17 ms, p90 10 ms, p95 15 ms, p99 17 ms, max 18 ms, mean 7.79 ms
- **Cassandra 5.0 (k3s)** (node2): n=30, min 2.25 ms, p50 3.52 ms, p90 3.90 ms, p95 3.95 ms, p99 9.83 ms, max 12 ms, mean 3.68 ms
- **Cassandra 5.0 (k3s)** (node3): n=30, min 2.18 ms, p50 3.18 ms, p90 3.47 ms, p95 3.72 ms, p99 12 ms, max 15 ms, mean 3.46 ms

## Failover detail

- **MongoDB 7.0**: killed primary; 144 write attempts until success; downtime 30.50 s; new primary replica; verified via replica. old primary restarted; whether it rejoins depends on the engine (stack should be recreated for reuse)
- **MongoDB 7.0 (k3s)**: killed primary; 1 write attempts until success; downtime 923 ms; new primary replica; verified via replica. old primary restarted; whether it rejoins depends on the engine (stack should be recreated for reuse)
- **Neo4j 5 Enterprise cluster**: killed node1; 1 write attempts until success; downtime 20.90 s; new primary node2; verified via router2. old primary restarted; whether it rejoins depends on the engine (stack should be recreated for reuse)
- **Neo4j 5 Enterprise cluster (k3s)**: killed node1; 1 write attempts until success; downtime 11.26 s; new primary node2; verified via router2. old primary restarted; whether it rejoins depends on the engine (stack should be recreated for reuse)
- **Redis 8**: killed primary; 12 write attempts until success; downtime 2.50 s; new primary replica; verified via replica. old primary restarted; whether it rejoins depends on the engine (stack should be recreated for reuse)
- **Redis 8 (k3s)**: killed primary; 12 write attempts until success; downtime 3.18 s; new primary replica; verified via replica. old primary restarted; whether it rejoins depends on the engine (stack should be recreated for reuse)
- **Elasticsearch 9**: killed node3; 1 write attempts until success; downtime 4.87 s; new primary primary; verified via node2. old primary restarted; whether it rejoins depends on the engine (stack should be recreated for reuse)
- **Elasticsearch 9 (k3s)**: killed node3; 1 write attempts until success; downtime 1.05 s; new primary node3; verified via node2. old primary restarted; whether it rejoins depends on the engine (stack should be recreated for reuse)
- **Cassandra 5.0**: killed primary; 1 write attempts until success; downtime 900 ms; new primary node2; verified via node2. old primary restarted; whether it rejoins depends on the engine (stack should be recreated for reuse)
- **Cassandra 5.0 (k3s)**: killed primary; 1 write attempts until success; downtime 1.61 s; new primary node2; verified via node2. old primary restarted; whether it rejoins depends on the engine (stack should be recreated for reuse)
