# Scenario: "GraphRec" — recommendations + fraud detection (Neo4j)

Neo4j's two flagship production workloads, on the e-commerce graph the lab already generates: **product
recommendations** (co-purchase / collaborative filtering) and **fraud-ring / identity resolution** — both multi-hop
graph traversals that are painful in every other engine and native here. Built Neo4j-first with an engine-agnostic
operation contract (`harness/nslab/scenarios/recgraph.py` `RecGraphBackend`), so another graph engine can implement the
same scenario later.

## Run it
```bash
uv run --project harness nslab up neo4j
uv run --project harness nslab scenario seed  neo4j --scenario recgraph --size small       # smoke | small | full
uv run --project harness nslab scenario run   neo4j --scenario recgraph --size small --seconds 20 --workers 8,32,64
uv run --project harness nslab scenario curves neo4j --scenario recgraph --size small       # reco-vs-popularity, ring-vs-depth
uv run --project harness nslab scenario drop  neo4j --scenario recgraph
```
Sizes (from the shared `datagen`): `smoke` 1k customers / 500 products / 10k orders; `small` 6k / 1.5k / 60k; `full`
20k / 5k / 200k. Results: `results/neo4j/scenario-recgraph-*.json`.

## Graph model (property graph)
```
(:Customer)-[:PLACED]->(:Order)-[:CONTAINS {qty}]->(:Product)-[:IN_CATEGORY]->(:Category)-[:CHILD_OF]->(:Category)
(:Customer)-[:USED]->(:Card)      (:Customer)-[:FROM_DEVICE]->(:Device)      (:Customer)-[:SHIPS_TO]->(:Address)
(:Customer)-[:RATED {stars}]->(:Product)
```
A uniqueness constraint on every node key both enforces identity and provides the index the traversals seek on. The
fraud/identity layer is the point: cards/devices/addresses are drawn from a **smaller Zipfian pool** than the customer
set, so many accounts share an identifier — real fraud rings and hot identifiers to detect.

## Operations
- **Recommendations** — `also_bought` (2-hop co-purchase collaborative filtering), `for_you` (personalized: products
  co-bought with what you own, that you don't), `similar_products` (same category), `trending_in_category`.
- **Fraud / identity** — `fraud_ring` (accounts sharing a card/device/address), `ring_expand` (variable-length
  `*2..2N` traversal through shared identifiers), `shortest_path` (degrees of separation via `shortestPath`),
  `shared_id_accounts` (hot-identifier fan-in).
- **General / writes** — `category_path` (variable-length up the category tree), `customer_orders`, `place_order`
  (create order + `CONTAINS` edges), `rate_product`.

Load: weighted read-heavy mix (recommendations 48%, fraud/identity 28%, traversal/serve 12%, writes 12%), Zipfian over
customers/products, worker ramp.

## Measured (small: 6k customers · 1.5k products · 60k orders · 146k co-purchase edges · 26k identity edges; Neo4j 5.26 Community, one 28-core host)

Seeded the whole graph in ~15 s. Steady-state ramp (zero errors):

| workers | throughput | `for_you` p50/p95 | `also_bought` p50/p95 | `fraud_ring` p95 | `ring_expand` p95 | `shortest_path` p95 | `similar` p95 |
|---:|---:|---|---|---:|---:|---:|---:|
| 8  | 418 ops/s | 108 / 203 ms | 6.5 / 10 ms | 4 ms | 4 ms | 4 ms | 3 ms |
| 32 | **756 ops/s** | 241 / 475 ms | 16 / 31 ms | 9 ms | 9 ms | 10 ms | 9 ms |
| 64 | 705 ops/s | 506 / **1184 ms** | 29 / 71 ms | 16 ms | 16 ms | 18 ms | 15 ms |

**What it shows**
- **The traversals Neo4j is built for stay cheap and predictable** even under load: fraud-ring detection, variable-depth
  ring expansion, shortest-path (degrees of separation), similar-products and order history are all **single-digit to
  low-tens of milliseconds** — this is index-free adjacency paying off. On Cassandra the equivalent "people you may
  know" 2-hop fan-out already cost 9–137 ms *without* concurrency and has no native path/traversal query at all.
- **The one expensive query is unbounded multi-hop collaborative filtering** (`for_you`: 4 hops across the whole
  co-purchase graph). It dominates CPU and its tail blows up from 203 ms → **1.18 s p95** as workers climb — exactly the
  query you'd **precompute / cache** (or run in Graph Data Science offline) rather than serve live.
- **Throughput** peaks ~**756 traversal-heavy ops/s** on a single Community node, CPU-bound at ~24 of 28 cores — vertical,
  because Community is single-instance (read replicas / causal clustering are Enterprise).

## Analysis curves
- **Recommendation latency vs product popularity**: 12–15 ms p95 across the catalog (26–100 and 101–500 purchase
  buckets). It is *flat* because the shared `datagen` picks order items uniformly, so there is no viral long-tail — a
  property of the dataset, and a fair result: co-purchase recommendation is stable when neighborhoods are similar-sized.
- **Ring-expansion latency vs traversal depth**: ~4.5 ms p95 at 2, 3 and 4 hops. Flat because the query is bounded
  (`LIMIT` on distinct ring members) and the identity graph is densely clustered around shared identifiers, so a
  bounded ring lookup short-circuits quickly regardless of nominal depth — the intended behaviour for a real-time
  fraud check (you bound it, then escalate deep analysis offline).

_Neo4j Community is a single instance, so these are single-node figures; horizontal read scaling and online backup are
Enterprise. Raw data: `results/neo4j/scenario-recgraph-*.json`._
