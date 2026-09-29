# Scenario: "Chatter" — a social messaging store (Cassandra first)

A realistic, domain-shaped workload that goes beyond the generic operation catalog: it models a chat app with a social
graph and exercises Cassandra the way Discord / WhatsApp DMs / Instagram do. Two capability groups — **find-friends**
(register, friend requests, friends/mutuals, "people you may know") and **messaging** (1:1 + group send, history, inbox,
unread, receipts, typing) — driven under a weighted, hotspot-skewed, multi-process load.

Built Cassandra-first, but the operations are defined engine-agnostically (`harness/nslab/scenarios/messaging.py`
`MessagingBackend`), so ScyllaDB reuses the same backend and MongoDB/Redis can implement the contract later for a
cross-engine comparison.

## Run it
```bash
uv run --project harness nslab up cassandra
uv run --project harness nslab scenario seed  cassandra --size small          # smoke | small | full
uv run --project harness nslab scenario run   cassandra --size small --seconds 25 --workers 8,32,64,128 --fanout write
uv run --project harness nslab scenario run   cassandra --size small --fanout read       # the fan-in-on-read comparison
uv run --project harness nslab scenario curves cassandra --size small --fanout write     # send-vs-group-size, pymk-vs-degree
uv run --project harness nslab scenario run   cassandra --rate 5000 --workers 64          # open-loop at a target ops/s
uv run --project harness nslab scenario drop  cassandra
```
Sizes: `smoke` 2k users / 5k conversations / ~0.33M messages; `small` 40k / 60k / ~8M; `full` 200k / 500k / tens of millions.
Results: `results/cassandra/scenario-messaging-*.json` (+ `-latest`, `-seed-*`, `-curves-*`).

## Data model (keyspace `chatter`, RF=3, one table per access path)
| table | key | serves |
|---|---|---|
| `users` / `users_by_username` | `(user_id)` / `(username)` | profile; **registration uniqueness via LWT** `IF NOT EXISTS` |
| `friends_by_user` | `((user_id), friend_id)` | a user's friends (adjacency list) |
| `friend_req_in` / `_out` | `((user_id), from_id)` / `((user_id), to_id)` | pending inbound/outbound requests |
| `conversations`, `conv_members`, `conv_members_by_user` | by conv / by user | roster + membership both directions |
| `messages_by_conv` | `((conversation_id, bucket), message_id DESC)` | history — Snowflake ids, 10-day buckets keep a partition bounded |
| `inbox_by_user` | `((user_id), conversation_id)` | inbox with a materialised last-message pointer (fan-out-on-write) |
| `unread` | `((user_id), conversation_id) → counter` | unread badge |
| `receipts`, `typing` | by conv | last-read per member; typing rows `USING TTL 10` |

## What it stresses (and why it's a good Cassandra test)
1. **Fan-out-on-write vs fan-in-on-read** — `--fanout write` updates every member's inbox + unread counter on send
   (cheap reads, write amplification with group size); `--fanout read` writes only the message and recomputes each
   user's inbox at read time (cheap writes, expensive inbox). The lab runs both and the crossover is visible.
2. **Hot / wide partitions** — user ids and conversation membership are sampled with a power-law skew, so celebrities
   appear in many conversations and hot conversations get most of the messages.
3. **LWT contention** — username registration (`IF NOT EXISTS`) and friend-accept (a multi-partition `LOGGED BATCH`).
4. **Counters** — unread uses Cassandra's separate counter path; note "mark read" cannot *set* a counter to zero, it
   subtracts the current value (a real Cassandra limitation the scenario surfaces).
5. **Tombstones** — typing (`USING TTL`) and friend-accept deletes generate tombstones on hot partitions.
6. **A 2-hop graph query with no graph** — "people you may know" reads a user's friends then fans out to each friend's
   friends and counts mutuals, entirely app-side; its latency scales with friend degree (the natural contrast vs Neo4j).

## Workload
- **Op mix** (weights): load_history 25 · load_inbox 20 · send_message 15 · mark_read 12 · unread_badge 8 · typing 5 ·
  list_friends 4 · send_request 3 · accept_request 2 · mutual_friends 2 · people_you_may_know 2 · register(LWT) 1.
- **Targets** are chosen from the deterministic corpus with the same power-law skew as the seed, so hotspots are hit
  disproportionately.
- **Drive modes**: closed-loop (ramp `--workers`, optional `--think-ms`) or open-loop (`--rate` ops/s). Consistency is
  `LOCAL_QUORUM` for messages (set on the stack's `features.consistency`).
- **Metrics**: throughput (ops/s), per-operation p50/p95/p99, errors, per-container CPU cores during the run (cgroup),
  plus two analysis curves — **send latency vs group size** and **PYMK latency vs friend degree**.

## Measured (small: 40k users / 60k conversations / ~8M messages, Cassandra 5.0 RF=3, LOCAL_QUORUM, one 28-core host)

Seed: 40k users, ~1.0M friend edges, 60k conversations, **~8.0M messages at ~19k msgs/s** (10 processes). Zero errors
across every load run below (the RF=3 cluster held up at LOCAL_QUORUM throughout).

### Fan-out-on-write vs fan-in-on-read (the headline)
Steady-state mixed load, worker ramp; p50/p95 ms per operation, throughput in ops/s.

| fan-out | peak throughput | send p95 (8→128w) | load_inbox p95 (8→128w) | why |
|---|---:|---|---|---|
| **write** | **2 064 ops/s** @ 64w | 87 → 765 ms | 11 → 75 ms | send fans out to every member's inbox + unread counter (write amplification, tail grows under load); inbox is a single materialised partition read (cheap) |
| **read** | **712 ops/s** @ 128w | 2 → 29 ms | **406 → 3 052 ms** | send writes only the message (cheap); load_inbox re-reads each conversation's head at read time — that one op (20% of the mix) caps total throughput at ~⅓ of fan-out-on-write |

**Fan-out-on-write serves a read-heavy messenger ~3× faster** — which is exactly why real systems materialise the inbox
on write and special-case huge groups (the send tail) with a cap. Fan-in-on-read trades a 40× cheaper send for an inbox
read that collapses under concurrency.

### Send latency vs group size (fan-out cost curve, write mode)
| group size | dm (2) | 3–5 | 6–20 | 21–50 | 51–150 | 150+ |
|---|---:|---:|---:|---:|---:|---:|
| send p95 ms | 7 | 9 | 21 | 36 | 108 | 197 |

Write amplification is roughly linear in group size — the celebrity / large-group hotspot, measured. This is the cost
the group-size cap (and a separate "large group" path in production) exists to bound.

### PYMK latency vs friend degree (2-hop fan-out, no graph engine)
| friend degree | ≤10 | 11–50 | 51–150 | 151–500 | 500+ |
|---|---:|---:|---:|---:|---:|
| PYMK p95 ms | 9 | 27 | 73 | 107 | 143 |

"People you may know" reads the user's friends then fans out to each friend's friends and counts mutuals — entirely
application-side. Its latency scales with degree because there is no server-side traversal; a celebrity's recommendation
is 15× more expensive than a light user's. This is the operation you would precompute/cache, or push to a graph engine
(the natural contrast against Neo4j in this lab).

### Per-operation cost (fan-out-write, 64 workers, p95 ms)
send_message 302 · people_you_may_know 302 · load_history 83 · accept_request(LOGGED BATCH) 51 · load_inbox 44 ·
register(LWT) 42 · mark_read(counter) 34. The LWT (username uniqueness) and LOGGED BATCH (friend accept) paths carry the
Paxos / batch-log overhead you expect; the counter "reset" in mark_read is a read-then-subtract (Cassandra counters
cannot be set), which the scenario surfaces as a real modelling constraint.

_Numbers were gathered with the three Cassandra nodes co-located on one host, so per-node CPU (≈5 cores each during the
ramp) — not the network — is the ceiling; treat throughput as a single-box figure. Re-run per size/consistency with the
commands above; raw data in `results/cassandra/scenario-messaging-*.json`._
