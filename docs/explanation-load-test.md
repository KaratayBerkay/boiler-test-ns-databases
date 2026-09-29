# Multi-connection bulk-insert load test

1000-doc batches, forked multi-process workers, best docs/s per engine. Batch p50 is the latency of one bulk
request. Busiest container = the container consuming the most CPU cores during the run.

| engine | entry point | workers | docs/s | batch p50 | errors | busiest container (cores) |
|---|---|---:|---:|---:|---:|---|
| MongoDB 7.0 | primary | 4 | 83,800 | 31 ms | 0 | mongo2 13.77 |
| MongoDB 7.0 | primary | 16 | 135,600 | 106 ms | 0 | mongo2 25.25 |
| MongoDB 7.0 | primary | 32 | 156,000 | 195 ms | 0 | mongo2 30.36 |
| MongoDB 7.0 (k3s) | primary | 4 | 103,400 | 32 ms | 0 | — |
| MongoDB 7.0 (k3s) | primary | 16 | 117,467 | 113 ms | 0 | — |
| MongoDB 7.0 (k3s) | primary | 32 | 138,267 | 206 ms | 0 | — |
| Neo4j 5 | primary | 4 | 14,067 | 135 ms | 0 | neo4j 3.94 |
| Neo4j 5 | primary | 16 | 11,533 | 208 ms | 0 | neo4j 4.52 |
| Neo4j 5 | primary | 32 | 14,600 | 207 ms | 1 | neo4j 5.81 |
| Neo4j 5 (k3s) | primary | 4 | 14,200 | 137 ms | 0 | — |
| Neo4j 5 (k3s) | primary | 16 | 14,000 | 198 ms | 0 | — |
| Neo4j 5 (k3s) | primary | 32 | 11,267 | 290 ms | 2 | — |
| Neo4j 5 Enterprise cluster | primary | 4 | 11,067 | 167 ms | 0 | neo4j3 3.73 |
| Neo4j 5 Enterprise cluster | primary | 16 | 9,267 | 307 ms | 0 | neo4j3 4.50 |
| Neo4j 5 Enterprise cluster | primary | 32 | 9,600 | 1.42 s | 0 | neo4j3 5.04 |
| Neo4j 5 Enterprise cluster (k3s) | primary | 4 | 12,467 | 156 ms | 0 | — |
| Neo4j 5 Enterprise cluster (k3s) | primary | 16 | 11,733 | 213 ms | 0 | — |
| Neo4j 5 Enterprise cluster (k3s) | primary | 32 | 11,267 | 374 ms | 1 | — |
| Redis 8 | primary | 4 | 61,800 | 59 ms | 0 | redis-primary 3.41 |
| Redis 8 | primary | 16 | 89,733 | 177 ms | 0 | redis-primary 6.42 |
| Redis 8 | primary | 32 | 89,867 | 352 ms | 0 | redis-primary 6.07 |
| Redis 8 | haproxy-ro | 4 | 44,733 | 60 ms | 21 | redis-primary 3.12 |
| Redis 8 | haproxy-ro | 16 | 81,800 | 94 ms | 168 | redis-primary 4.64 |
| Redis 8 | haproxy-ro | 32 | 78,200 | 192 ms | 336 | redis-primary 5.92 |
| Redis 8 (k3s) | primary | 4 | 63,267 | 57 ms | 0 | — |
| Redis 8 (k3s) | primary | 16 | 86,533 | 184 ms | 0 | — |
| Redis 8 (k3s) | primary | 32 | 89,600 | 353 ms | 0 | — |
| Elasticsearch 9 | primary | 4 | 40,200 | 97 ms | 0 | es1 3.22 |
| Elasticsearch 9 | primary | 16 | 111,533 | 102 ms | 0 | es1 10.95 |
| Elasticsearch 9 | primary | 32 | 106,333 | 228 ms | 0 | es1 12.73 |
| Elasticsearch 9 (k3s) | primary | 4 | 36,000 | 107 ms | 0 | — |
| Elasticsearch 9 (k3s) | primary | 16 | 120,200 | 102 ms | 0 | — |
| Elasticsearch 9 (k3s) | primary | 32 | 111,800 | 207 ms | 0 | — |
| Cassandra 5.0 | primary | 4 | 15,200 | 244 ms | 0 | cassandra1 8.18 |
| Cassandra 5.0 | primary | 16 | 31,467 | 496 ms | 0 | cassandra1 11.73 |
| Cassandra 5.0 | primary | 32 | 33,667 | 938 ms | 0 | cassandra1 12.26 |
| Cassandra 5.0 (k3s) | primary | 4 | 13,200 | 250 ms | 0 | — |
| Cassandra 5.0 (k3s) | primary | 16 | 28,267 | 430 ms | 0 | — |
| Cassandra 5.0 (k3s) | primary | 32 | 23,667 | 970 ms | 0 | — |
