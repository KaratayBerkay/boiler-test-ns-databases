# Scenarios

Domain workloads beyond the generic operation catalog: a weighted, hotspot-skewed, multi-process load per engine.

## Scenario — Chatter on Cassandra 5.0

social messaging store (register / find-friends + 1:1 & group chat) — a weighted, hotspot-skewed, multi-process load beyond the generic operation catalog.

seed: 40,000 users · 999,976 friends · 119,996 requests · 60,000 conversations · 7,983,898 messages (18,969 msgs/s). Cassandra 5.0.9 keyspace=chatter RF=3 CL=6 fanout=write

### workers

| workers | throughput ops/s | errors | load_history p95 | load_inbox p95 | send_message p95 | mark_read p95 | unread_badge p95 | typing p95 | list_friends p95 | send_request p95 | accept_request p95 | mutual_friends p95 | people_you_may_know p95 | register p95 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 1,322 | 0 | 10 ms | 11 ms | 84 ms | 4.25 ms | 3.91 ms | 1.23 ms | 2.70 ms | 2.79 ms | 4.39 ms | 9.24 ms | 91 ms | 3.89 ms |
| 32 | 1,904 | 0 | 39 ms | 30 ms | 167 ms | 17 ms | 12 ms | 5.45 ms | 11 ms | 13 ms | 20 ms | 27 ms | 157 ms | 13 ms |
| 64 | 2,073 | 0 | 82 ms | 46 ms | 292 ms | 35 ms | 26 ms | 13 ms | 22 ms | 32 ms | 53 ms | 51 ms | 301 ms | 26 ms |

### workers

| workers | throughput ops/s | errors | load_history p95 | load_inbox p95 | send_message p95 | mark_read p95 | unread_badge p95 | typing p95 | list_friends p95 | send_request p95 | accept_request p95 | mutual_friends p95 | people_you_may_know p95 | register p95 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 360 | 0 | 11 ms | 457 ms | 2.14 ms | 4.73 ms | 5.14 ms | 1.79 ms | 3.19 ms | 4.02 ms | 8.05 ms | 10 ms | 91 ms | 4.93 ms |
| 32 | 632 | 0 | 50 ms | 891 ms | 9.63 ms | 21 ms | 15 ms | 8.19 ms | 12 ms | 22 ms | 30 ms | 34 ms | 276 ms | 17 ms |
| 64 | 637 | 0 | 57 ms | 1.79 s | 14 ms | 28 ms | 19 ms | 15 ms | 16 ms | 31 ms | 63 ms | 45 ms | 679 ms | 24 ms |

### Send latency vs group size

| bucket | n | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|
| dm(2) | 288 | 4.56 ms | 6.85 ms | 7.44 ms | 7.96 ms |
| 3-5 | 25 | 6.15 ms | 8.94 ms | 11 ms | 11 ms |
| 6-20 | 17 | 12 ms | 16 ms | 16 ms | 17 ms |
| 21-50 | 22 | 23 ms | 32 ms | 39 ms | 41 ms |
| 51-150 | 28 | 58 ms | 98 ms | 110 ms | 113 ms |
| 150+ | 20 | 124 ms | 177 ms | 190 ms | 193 ms |

### PYMK latency vs friend degree

| bucket | n | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|
| <=10 | 6 | 11 ms | 17 ms | 18 ms | 18 ms |
| 11-50 | 287 | 17 ms | 29 ms | 40 ms | 56 ms |
| 51-150 | 60 | 43 ms | 77 ms | 86 ms | 92 ms |
| 151-500 | 31 | 96 ms | 123 ms | 131 ms | 134 ms |
| 500+ | 16 | 105 ms | 137 ms | 138 ms | 138 ms |

## Scenario — GraphRec on Neo4j 5

product recommendations (co-purchase) + fraud-ring / identity detection — a weighted, hotspot-skewed, multi-process load beyond the generic operation catalog.

seed: 6,000 customers · 200 categories · 1,500 products · 60,000 orders · 6,000 identifiers · 24,000 ratings. Neo4j Kernel 5.26.30 (community) — recgraph

### workers

| workers | throughput ops/s | errors | also_bought p95 | for_you p95 | fraud_ring p95 | similar_products p95 | customer_orders p95 | place_order p95 | trending_in_category p95 | ring_expand p95 | shared_id_accounts p95 | shortest_path p95 | category_path p95 | rate_product p95 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 418 | 0 | 10 ms | 203 ms | 3.73 ms | 2.49 ms | 3.01 ms | 4.57 ms | 3.29 ms | 3.93 ms | 2.09 ms | 3.58 ms | 2.28 ms | 3.51 ms |
| 32 | 756 | 0 | 31 ms | 475 ms | 9.11 ms | 8.60 ms | 8.49 ms | 20 ms | 9.36 ms | 8.97 ms | 8.49 ms | 9.53 ms | 8.80 ms | 14 ms |
| 64 | 705 | 0 | 71 ms | 1.18 s | 16 ms | 15 ms | 15 ms | 37 ms | 15 ms | 16 ms | 14 ms | 18 ms | 15 ms | 23 ms |

### Recommendation latency vs product popularity

| bucket | n | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|
| <=5 | 0 | — | — | — | — |
| 6-25 | 0 | — | — | — | — |
| 26-100 | 144 | 11 ms | 13 ms | 13 ms | 14 ms |
| 101-500 | 356 | 12 ms | 15 ms | 19 ms | 30 ms |
| 500+ | 0 | — | — | — | — |

### Fraud ring-expansion latency vs traversal depth

| bucket | n | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|
| 2_hops | 174 | 4.02 ms | 4.46 ms | 4.91 ms | 5.21 ms |
| 3_hops | 163 | 4.03 ms | 4.45 ms | 5.04 ms | 5.33 ms |
| 4_hops | 163 | 4.06 ms | 4.54 ms | 5.10 ms | 5.34 ms |

## Scenario — Chatter on Cassandra 5.0 (k3s)

social messaging store (register / find-friends + 1:1 & group chat) — on k3s — a weighted, hotspot-skewed, multi-process load beyond the generic operation catalog.

seed: 40,000 users · 999,976 friends · 119,996 requests · 60,000 conversations · 7,983,898 messages (15,466 msgs/s). Cassandra 5.0.9 keyspace=chatter RF=3 CL=6 fanout=write

### workers

| workers | throughput ops/s | errors | load_history p95 | load_inbox p95 | send_message p95 | mark_read p95 | unread_badge p95 | typing p95 | list_friends p95 | send_request p95 | accept_request p95 | mutual_friends p95 | people_you_may_know p95 | register p95 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 1,117 | 0 | 15 ms | 12 ms | 91 ms | 5.44 ms | 3.05 ms | 1.92 ms | 3.92 ms | 5.21 ms | 8.23 ms | 11 ms | 100 ms | 9.63 ms |
| 32 | 2,605 | 0 | 40 ms | 19 ms | 119 ms | 13 ms | 7.86 ms | 4.45 ms | 8.71 ms | 12 ms | 19 ms | 25 ms | 113 ms | 20 ms |
| 64 | 2,936 | 0 | 70 ms | 28 ms | 143 ms | 27 ms | 16 ms | 8.36 ms | 16 ms | 22 ms | 37 ms | 45 ms | 139 ms | 33 ms |

### Send latency vs group size

| bucket | n | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|
| dm(2) | 288 | 4.28 ms | 7.81 ms | 8.64 ms | 12 ms |
| 3-5 | 25 | 6.07 ms | 11 ms | 11 ms | 11 ms |
| 6-20 | 17 | 12 ms | 21 ms | 35 ms | 38 ms |
| 21-50 | 22 | 19 ms | 33 ms | 34 ms | 34 ms |
| 51-150 | 28 | 55 ms | 78 ms | 102 ms | 110 ms |
| 150+ | 20 | 127 ms | 162 ms | 173 ms | 176 ms |

### PYMK latency vs friend degree

| bucket | n | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|
| <=10 | 6 | 7.55 ms | 14 ms | 15 ms | 16 ms |
| 11-50 | 287 | 15 ms | 28 ms | 39 ms | 49 ms |
| 51-150 | 61 | 36 ms | 69 ms | 90 ms | 111 ms |
| 151-500 | 30 | 88 ms | 123 ms | 129 ms | 131 ms |
| 500+ | 16 | 103 ms | 163 ms | 174 ms | 177 ms |

## Scenario — GraphRec on Neo4j 5 (k3s)

product recommendations (co-purchase) + fraud-ring / identity detection — on k3s — a weighted, hotspot-skewed, multi-process load beyond the generic operation catalog.

seed: 6,000 customers · 200 categories · 1,500 products · 60,000 orders · 6,000 identifiers · 24,000 ratings. Neo4j Kernel 5.26.30 (community) — recgraph

### workers

| workers | throughput ops/s | errors | also_bought p95 | for_you p95 | fraud_ring p95 | similar_products p95 | customer_orders p95 | place_order p95 | trending_in_category p95 | ring_expand p95 | shared_id_accounts p95 | shortest_path p95 | category_path p95 | rate_product p95 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 427 | 0 | 9.63 ms | 193 ms | 4.33 ms | 2.96 ms | 3.53 ms | 5.64 ms | 3.82 ms | 4.99 ms | 2.59 ms | 4.90 ms | 3.11 ms | 5.36 ms |
| 32 | 922 | 0 | 25 ms | 353 ms | 9.09 ms | 8.23 ms | 8.41 ms | 22 ms | 9.84 ms | 9.24 ms | 8.49 ms | 10 ms | 8.13 ms | 19 ms |

### Recommendation latency vs product popularity

| bucket | n | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|
| <=5 | 0 | — | — | — | — |
| 6-25 | 0 | — | — | — | — |
| 26-100 | 197 | 10 ms | 13 ms | 14 ms | 69 ms |
| 101-500 | 203 | 11 ms | 14 ms | 17 ms | 18 ms |
| 500+ | 0 | — | — | — | — |

### Fraud ring-expansion latency vs traversal depth

| bucket | n | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|
| 2_hops | 134 | 3.94 ms | 5.43 ms | 5.80 ms | 5.93 ms |
| 3_hops | 131 | 4.02 ms | 5.45 ms | 5.94 ms | 6.16 ms |
| 4_hops | 135 | 3.96 ms | 5.47 ms | 5.74 ms | 5.85 ms |

