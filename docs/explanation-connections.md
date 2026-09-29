# Connections & concurrency

Forked multi-process workers doing point reads. Connect p50 is one fresh connection; throughput is ops/s at
1 / 16 / 64 workers. CAS contention: 4 threads racing the same document with compare-and-set loops
(applied + rejected, and whether every accepted write advanced the version exactly once = atomic).

| engine | max conn | connect p50 | 1 worker ops/s | 16w ops/s | 64w ops/s | CAS contention |
|---|---:|---:|---:|---:|---:|---|
| MongoDB 7.0 | 5,000 | 3.90 ms | 786 | 23,845 | 30,570 | 96/104 atomic |
| MongoDB 7.0 (k3s) | 409 | 4.13 ms | 898 | 21,419 | 24,361 | 89/111 atomic |
| LMDB | — | 0.00 ms | 98,712 | 29,189 | 29,338 | 200/0 not atomic |
| RocksDB | — | 0.00 ms | 74,229 | 33,585 | 34,356 | 200/0 atomic |
| Neo4j 5 | 800 | 5.92 ms | 520 | 12,806 | 15,302 | 200/0 atomic |
| Neo4j 5 (k3s) | 800 | 7.08 ms | 457 | 11,443 | 12,002 | 200/0 atomic |
| Neo4j 5 Enterprise cluster | 800 | 18 ms | 447 | 7,784 | 7,606 | 200/0 atomic |
| Neo4j 5 Enterprise cluster (k3s) | 800 | 17 ms | 515 | 7,095 | 8,613 | 200/0 atomic |
| Redis 8 | 10,000 | 2.22 ms | 2,186 | 60,747 | 79,313 | 69/131 atomic |
| Redis 8 (k3s) | 10,000 | 2.96 ms | 1,895 | 51,369 | 67,202 | 69/131 atomic |
| Elasticsearch 9 | — | 3.08 ms | 330 | 9,748 | 12,493 | 98/102 not atomic |
| Elasticsearch 9 (k3s) | — | 6.47 ms | 308 | 9,372 | 11,716 | 72/128 not atomic |
| Cassandra 5.0 | — | 30 ms | 625 | 12,057 | 15,714 | 146/54 atomic |
| Cassandra 5.0 (k3s) | — | 29 ms | 426 | 12,336 | 15,310 | 134/66 atomic |

## Full concurrency curves

### MongoDB 7.0 — primary

| workers | ops/s | p50 ms | p99 ms | errors |
|---:|---:|---:|---:|---:|
| 1 | 786 | 1.39 ms | 1.78 ms | 0 |
| 4 | 3,500 | 1.23 ms | 1.64 ms | 0 |
| 16 | 23,845 | 0.62 ms | 1.09 ms | 0 |
| 32 | 27,956 | 1.07 ms | 2.31 ms | 0 |
| 64 | 30,570 | 1.88 ms | 5.42 ms | 0 |

### MongoDB 7.0 (k3s) — primary

| workers | ops/s | p50 ms | p99 ms | errors |
|---:|---:|---:|---:|---:|
| 1 | 898 | 1.18 ms | 1.56 ms | 0 |
| 4 | 4,252 | 0.90 ms | 1.52 ms | 0 |
| 16 | 21,419 | 0.68 ms | 1.41 ms | 0 |
| 32 | 23,781 | 1.18 ms | 2.79 ms | 0 |
| 64 | 24,361 | 2.05 ms | 7.04 ms | 0 |

### LMDB — embedded

| workers | ops/s | p50 ms | p99 ms | errors |
|---:|---:|---:|---:|---:|
| 1 | 98,712 | 0.01 ms | 0.02 ms | 0 |
| 4 | 52,969 | 0.02 ms | 0.05 ms | 0 |
| 16 | 29,189 | 0.03 ms | 15 ms | 0 |
| 32 | 28,948 | 0.03 ms | 32 ms | 0 |
| 64 | 29,338 | 0.03 ms | 67 ms | 0 |

### RocksDB — embedded

| workers | ops/s | p50 ms | p99 ms | errors |
|---:|---:|---:|---:|---:|
| 1 | 74,229 | 0.01 ms | 0.03 ms | 0 |
| 4 | 34,828 | 0.03 ms | 0.08 ms | 0 |
| 16 | 33,585 | 0.03 ms | 0.08 ms | 0 |
| 32 | 35,889 | 0.03 ms | 0.07 ms | 0 |
| 64 | 34,356 | 0.03 ms | 0.08 ms | 0 |

### Neo4j 5 — primary

| workers | ops/s | p50 ms | p99 ms | errors |
|---:|---:|---:|---:|---:|
| 1 | 520 | 2.03 ms | 2.92 ms | 0 |
| 4 | 2,465 | 1.60 ms | 2.68 ms | 0 |
| 16 | 12,806 | 1.18 ms | 2.11 ms | 0 |
| 32 | 13,555 | 2.21 ms | 4.91 ms | 0 |
| 64 | 15,302 | 3.69 ms | 12 ms | 0 |

### Neo4j 5 (k3s) — primary

| workers | ops/s | p50 ms | p99 ms | errors |
|---:|---:|---:|---:|---:|
| 1 | 457 | 2.16 ms | 3.07 ms | 0 |
| 4 | 2,474 | 1.56 ms | 2.49 ms | 0 |
| 16 | 11,443 | 1.26 ms | 2.33 ms | 0 |
| 32 | 11,969 | 2.32 ms | 5.24 ms | 0 |
| 64 | 12,002 | 4.08 ms | 13 ms | 0 |

### Neo4j 5 Enterprise cluster — primary

| workers | ops/s | p50 ms | p99 ms | errors |
|---:|---:|---:|---:|---:|
| 1 | 447 | 2.14 ms | 3.22 ms | 0 |
| 4 | 2,141 | 1.62 ms | 3.02 ms | 0 |
| 16 | 7,784 | 1.73 ms | 3.79 ms | 0 |
| 32 | 8,521 | 2.99 ms | 7.50 ms | 0 |
| 64 | 7,606 | 5.46 ms | 20 ms | 0 |

### Neo4j 5 Enterprise cluster (k3s) — primary

| workers | ops/s | p50 ms | p99 ms | errors |
|---:|---:|---:|---:|---:|
| 1 | 515 | 1.87 ms | 3.18 ms | 0 |
| 4 | 2,579 | 1.48 ms | 2.44 ms | 0 |
| 16 | 7,095 | 2.06 ms | 4.70 ms | 0 |
| 32 | 7,484 | 3.87 ms | 11 ms | 0 |
| 64 | 8,613 | 6.36 ms | 24 ms | 0 |

### Redis 8 — primary

| workers | ops/s | p50 ms | p99 ms | errors |
|---:|---:|---:|---:|---:|
| 1 | 2,186 | 0.42 ms | 0.81 ms | 0 |
| 4 | 9,228 | 0.43 ms | 0.75 ms | 0 |
| 16 | 60,747 | 0.25 ms | 0.55 ms | 0 |
| 32 | 68,425 | 0.44 ms | 0.95 ms | 0 |
| 64 | 79,313 | 0.76 ms | 1.69 ms | 0 |

### Redis 8 — haproxy-ro

| workers | ops/s | p50 ms | p99 ms | errors |
|---:|---:|---:|---:|---:|
| 1 | 1,423 | 0.75 ms | 1.07 ms | 0 |
| 4 | 6,231 | 0.67 ms | 1.00 ms | 0 |
| 16 | 42,156 | 0.36 ms | 0.74 ms | 0 |
| 32 | 48,866 | 0.62 ms | 1.30 ms | 0 |
| 64 | 58,280 | 1.03 ms | 2.37 ms | 0 |

### Redis 8 (k3s) — primary

| workers | ops/s | p50 ms | p99 ms | errors |
|---:|---:|---:|---:|---:|
| 1 | 1,895 | 0.53 ms | 0.89 ms | 0 |
| 4 | 8,522 | 0.46 ms | 0.83 ms | 0 |
| 16 | 51,369 | 0.30 ms | 0.57 ms | 0 |
| 32 | 58,733 | 0.52 ms | 1.06 ms | 0 |
| 64 | 67,202 | 0.91 ms | 1.91 ms | 0 |

### Redis 8 (k3s) — haproxy-ro

| workers | ops/s | p50 ms | p99 ms | errors |
|---:|---:|---:|---:|---:|
| 1 | 1,562 | 0.64 ms | 1.07 ms | 0 |
| 4 | 6,756 | 0.61 ms | 0.98 ms | 0 |
| 16 | 36,704 | 0.41 ms | 0.79 ms | 0 |
| 32 | 43,598 | 0.69 ms | 1.43 ms | 0 |
| 64 | 50,010 | 1.20 ms | 2.75 ms | 0 |

### Elasticsearch 9 — primary

| workers | ops/s | p50 ms | p99 ms | errors |
|---:|---:|---:|---:|---:|
| 1 | 330 | 3.14 ms | 4.00 ms | 0 |
| 4 | 1,655 | 2.48 ms | 3.62 ms | 0 |
| 16 | 9,748 | 1.56 ms | 3.31 ms | 0 |
| 32 | 11,514 | 2.58 ms | 5.57 ms | 0 |
| 64 | 12,493 | 4.83 ms | 11 ms | 0 |

### Elasticsearch 9 (k3s) — primary

| workers | ops/s | p50 ms | p99 ms | errors |
|---:|---:|---:|---:|---:|
| 1 | 308 | 3.31 ms | 4.35 ms | 0 |
| 4 | 1,488 | 2.80 ms | 3.89 ms | 0 |
| 16 | 9,372 | 1.63 ms | 3.35 ms | 0 |
| 32 | 11,565 | 2.60 ms | 5.29 ms | 0 |
| 64 | 11,716 | 5.00 ms | 14 ms | 0 |

### Cassandra 5.0 — primary

| workers | ops/s | p50 ms | p99 ms | errors |
|---:|---:|---:|---:|---:|
| 1 | 625 | 1.61 ms | 2.47 ms | 0 |
| 4 | 3,382 | 1.08 ms | 2.00 ms | 0 |
| 16 | 12,057 | 1.24 ms | 2.43 ms | 0 |
| 32 | 16,531 | 1.82 ms | 3.81 ms | 0 |
| 64 | 15,714 | 3.60 ms | 11 ms | 0 |

### Cassandra 5.0 (k3s) — primary

| workers | ops/s | p50 ms | p99 ms | errors |
|---:|---:|---:|---:|---:|
| 1 | 426 | 2.39 ms | 3.10 ms | 0 |
| 4 | 2,452 | 1.52 ms | 2.67 ms | 0 |
| 16 | 12,336 | 1.20 ms | 2.49 ms | 0 |
| 32 | 16,762 | 1.84 ms | 3.20 ms | 0 |
| 64 | 15,310 | 3.92 ms | 8.74 ms | 0 |

