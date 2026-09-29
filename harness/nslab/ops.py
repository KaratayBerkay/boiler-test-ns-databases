"""Operation catalog: the cross-engine workload.

NoSQL engines share no query language, so instead of SQL text each operation is a *contract*: an id, a
description of what it must do against the portable data model (schema.py), and a parameter generator.
Every engine adapter implements the operations it can as `Client.op_<id>(params) -> rows`; an operation
without an implementation is reported as unsupported, one that raises `Unsupported` likewise, and one that
raises anything else as an error. Rows are lists of tuples so results can be checksummed across engines.

Id ranges used by write operations (kept far above the dataset so cleanup is exact):
  insert_one       events.id  50_000_000 + i
  delete_one       events.id  60_000_000 + i   (pre-seeded by op.setup)
  batch_insert     events.id  70_000_000 + i*1000 .. +999
  loadtest         events.id  40_000_000_000+   (phases/loadtest.py)
  repl probes      collection repl_probe
"""
from __future__ import annotations

import datetime as dt
import random
from dataclasses import dataclass, field
from typing import Any, Callable

from .datagen import Sizes, query_vector

Params = dict[str, Any]


@dataclass
class Op:
    id: str
    title: str
    kind: str                                   # read | write | txn
    tags: tuple[str, ...]
    contract: str                               # what an implementation must do (used by skills/docs)
    params: Callable[[Sizes, random.Random], list[Params]] | None = None
    warmup: int = 2
    iters: int = 10
    max_seconds: float = 20.0
    setup: str | None = None                    # Client method name called once before timing (receives params)
    cleanup: str | None = None                  # Client method name called once after timing (receives params)
    expect_rows: bool = True                    # False for pure writes (row count / checksum not meaningful)


def _ts(y: int, m: int, d: int) -> dt.datetime:
    return dt.datetime(y, m, d)


def p_ids(coll_size: int, n: int = 40):
    def f(sz: Sizes, r: random.Random):
        return [{"id": r.randint(1, coll_size(sz) if callable(coll_size) else coll_size)} for _ in range(n)]
    return f


def p_multi_get(sz: Sizes, r: random.Random):
    return [{"ids": sorted(r.sample(range(1, sz.customers + 1), 100))} for _ in range(20)]


def p_insert_one(sz: Sizes, r: random.Random):
    return [{"id": 50_000_000 + i, "customer_id": r.randint(1, sz.customers), "event_type": "bench_insert",
             "occurred_at": dt.datetime(2026, 1, 1, 0, 0, i % 60), "payload": {"page": "/bench", "ref": "direct", "device": "desktop"},
             "value_num": round(r.random() * 10, 4)} for i in range(40)]


def p_delete_one(sz: Sizes, r: random.Random):
    return [{"id": 60_000_000 + i, "customer_id": r.randint(1, sz.customers), "event_type": "bench_delete",
             "occurred_at": dt.datetime(2026, 1, 2), "payload": None, "value_num": 1.0} for i in range(40)]


def p_batch_insert(sz: Sizes, r: random.Random):
    out = []
    for b in range(15):
        base = 70_000_000 + b * 1000
        out.append({"base": base, "docs": [{"id": base + i, "customer_id": r.randint(1, sz.customers), "event_type": "bench_batch",
                                             "occurred_at": dt.datetime(2026, 1, 3, 0, (i // 60) % 60, i % 60), "payload": None,
                                             "value_num": float(i % 100)} for i in range(1000)]})
    return out


def p_update_field(sz: Sizes, r: random.Random):
    return [{"id": r.randint(1, sz.customers), "value": f"2026-01-01T00:00:{i % 60:02d}Z"} for i in range(40)]


def p_inventory(sz: Sizes, r: random.Random):
    return [{"product_id": r.randint(1, sz.products), "warehouse_id": r.randint(1, sz.warehouses), "qty": r.randint(100, 500)} for _ in range(40)]


def p_transfer(sz: Sizes, r: random.Random):
    out = []
    for _ in range(40):
        pid = r.randint(1, sz.products)
        w1, w2 = r.sample(range(1, sz.warehouses + 1), 2)
        out.append({"product_id": pid, "from": w1, "to": w2, "amount": 1})
    return out


def p_range_secondary(sz: Sizes, r: random.Random):
    return [{"customer_id": r.randint(1, sz.customers)} for _ in range(40)]


def p_range_time(sz: Sizes, r: random.Random):
    out = []
    for _ in range(20):
        start = dt.datetime(2025, r.randint(1, 12), r.randint(1, 28))
        out.append({"event_type": "checkout", "start": start, "end": start + dt.timedelta(days=3), "limit": 100})
    return out


def p_top_n(sz: Sizes, r: random.Random):
    return [{"category_id": r.randint(1, sz.categories), "n": 10} for _ in range(40)]


def p_pagination(sz: Sizes, r: random.Random):
    # keyset cursor is (ordered_at, id) of the row at offset 10000 in the deterministic dataset; adapters may derive their own
    return [{"offset": 10_000, "page": 50}]


def p_text(sz: Sizes, r: random.Random):
    return [{"words": w, "limit": 20} for w in (["wireless", "charger"], ["premium", "leather"], ["gaming", "bluetooth"], ["solar", "outdoor"])]


def p_geo(sz: Sizes, r: random.Random):
    # points near the jittered country centroids of datagen.COUNTRY_CENTROID (Berlin, Ankara, Kansas, Paris)
    return [{"lat": 52.5, "lon": 13.4, "km": 200, "limit": 20}, {"lat": 39.9, "lon": 32.9, "km": 200, "limit": 20},
            {"lat": 39.0, "lon": -98.0, "km": 200, "limit": 20}, {"lat": 48.8, "lon": 2.3, "km": 200, "limit": 20}]


def p_vector(sz: Sizes, r: random.Random):
    return [{"vector": query_vector(i, r.randint(1, sz.categories)), "k": 10} for i in range(20)]


def p_ttl(sz: Sizes, r: random.Random):
    return [{"key": f"ttl:{i}", "seconds": 120, "value": {"i": i, "note": "bench"}} for i in range(40)]


def p_graph(sz: Sizes, r: random.Random):
    return [{"product_id": r.randint(1, sz.products)} for _ in range(40)]


def p_join(sz: Sizes, r: random.Random):
    return [{"customer_id": r.randint(1, sz.customers)} for _ in range(40)]


def p_cas(sz: Sizes, r: random.Random):
    return [{"product_id": r.randint(1, sz.products), "warehouse_id": r.randint(1, sz.warehouses)} for _ in range(40)]


CATALOG: list[Op] = [
    Op("ping", "Round trip: cheapest server call", "read", ("connection",), "PING / hello / GET root; return one row", iters=30, expect_rows=False),
    Op("get_by_key", "Point read: customer by primary key", "read", ("kv", "read"),
       "return (id, name, email, country_code) of customers[id]", p_ids(lambda s: s.customers), iters=30),
    Op("multi_get", "Batch point read: 100 customers by key in one request", "read", ("kv", "read", "batch"),
       "return (id, tier) for 100 ids, ordered by id; one round trip (MGET / $in / IN / mget)", p_multi_get, iters=20),
    Op("insert_one", "Single document insert (event)", "write", ("write",),
       "insert one event document with the given id; acknowledged by the primary", p_insert_one, iters=30, cleanup="cleanup_events_by_params", expect_rows=False),
    Op("update_field", "Partial update: set one field of a customer", "write", ("write", "partial"),
       "set customers[id].last_login = value without rewriting the whole document", p_update_field, iters=30, expect_rows=False),
    Op("upsert", "Upsert inventory row by composite key", "write", ("write",),
       "inventory[(product_id, warehouse_id)] := {qty, updated_at=now} creating or replacing", p_inventory, iters=30, expect_rows=False),
    Op("delete_one", "Delete one document by key", "write", ("write",),
       "delete events[id] (rows pre-seeded by setup)", p_delete_one, iters=30, setup="setup_seed_events", expect_rows=False),
    Op("atomic_increment", "Server-side atomic increment of a numeric field", "write", ("write", "atomic"),
       "inventory[(product_id, warehouse_id)].qty += 1 atomically; return the new value", p_inventory, iters=30),
    Op("conditional_update", "Compare-and-set on a version field", "write", ("write", "atomic", "cas"),
       "read inventory row, update qty only if version unchanged, bump version; return (applied, new_version)", p_cas, iters=30),
    Op("batch_insert_1000", "Bulk write: 1000 events in one request", "write", ("write", "batch"),
       "insert 1000 event documents with one bulk/batch/pipeline call", p_batch_insert, warmup=1, iters=8, max_seconds=30,
       cleanup="cleanup_batch_events", expect_rows=False),
    Op("range_by_secondary", "Secondary lookup: orders of one customer", "read", ("read", "index"),
       "return (id, status, total_amount) of orders where customer_id = ?, ordered by id", p_range_secondary, iters=20),
    Op("range_by_time", "Range scan: events of one type in a 3-day window", "read", ("read", "range", "time"),
       "return up to 100 (id, customer_id, occurred_at) of events where event_type=? and start<=occurred_at<end ordered by occurred_at,id "
       "(no baseline index on this path; the optimisation phase adds one)", p_range_time, iters=10, max_seconds=30),
    Op("filter_nested", "Filter on a nested field + top-level field", "read", ("read", "nested"),
       "count customers where attrs.lang='tr' and tier='pro'", None, iters=10),
    Op("array_contains", "Filter on array membership", "read", ("read", "array"),
       "count products where attrs.tags contains 'eco' and active", None, iters=10),
    Op("top_n_sorted", "Top-N with sort: 10 most expensive active products of a category", "read", ("read", "sort"),
       "return (id, price) ordered by price desc, id asc limit 10 where category_id=? and active", p_top_n, iters=20),
    Op("pagination_offset", "Deep page via offset (skip 10000)", "read", ("read", "pagination"),
       "orders ordered by (ordered_at, id): skip 10000, return 50 (id, ordered_at)", p_pagination, iters=8, max_seconds=30),
    Op("pagination_keyset", "Deep page via keyset / cursor", "read", ("read", "pagination"),
       "same page as pagination_offset using a (ordered_at, id) cursor or engine-native search_after / paging state", p_pagination, iters=10),
    Op("count_filtered", "Count with equality filter", "read", ("read", "aggregate"),
       "count orders where status='pending'", None, iters=10),
    Op("aggregate_group", "Group-by aggregate over 200k orders", "read", ("read", "aggregate"),
       "orders grouped by shipping_country: (country, count, sum(total_amount)) ordered by country", None, iters=8, max_seconds=30),
    Op("aggregate_time_bucket", "Time-bucketed aggregate over 1M events", "read", ("read", "aggregate", "time"),
       "events where event_type='checkout' grouped by month of occurred_at: (yyyy-mm, count, sum(value_num)) ordered by month", None, iters=5, max_seconds=40),
    Op("join_lookup", "Orders of a customer with the customer's name (server-side join if the engine has one)", "read", ("read", "join"),
       "return (order_id, customer_name, total_amount) for customer_id=? ordered by order_id; implementations tag meta['join']='server'|'client'", p_join, iters=20),
    Op("graph_traversal", "Category ancestor chain of a product (variable-depth traversal)", "read", ("read", "graph"),
       "return the category path root..leaf as (depth, category_id) rows for products[id]", p_graph, iters=20),
    Op("text_search", "Full-text search on product descriptions", "read", ("read", "text"),
       "return up to 20 (id) of products whose description contains all given words (engine's text index or scan), ordered by id", p_text, iters=10),
    Op("geo_near", "Geo radius query on customer locations", "read", ("read", "geo"),
       "return up to 20 (id) of customers within km of (lat,lon), nearest first", p_geo, iters=10),
    Op("vector_knn", "k-nearest-neighbour search on 8-dim product embeddings", "read", ("read", "vector"),
       "return (id) of the 10 products with the highest cosine similarity to the query vector", p_vector, iters=20),
    Op("transaction_transfer", "Multi-document atomic transfer between two inventory rows", "txn", ("write", "txn"),
       "atomically qty -= 1 on (pid, from) and qty += 1 on (pid, to); both or neither", p_transfer, iters=20, expect_rows=False),
    Op("ttl_set", "Write with expiry", "write", ("write", "ttl"),
       "store value under key with a 120 s TTL (native TTL, or TTL index / expire field)", p_ttl, iters=30, expect_rows=False),
    Op("scan_full", "Full scan: count all events", "read", ("read", "scan"),
       "exact count of the events collection (1M docs) — measures scan / metadata-count cost", None, iters=5, max_seconds=40),
]

OP_BY_ID = {o.id: o for o in CATALOG}


class Unsupported(Exception):
    """Raised by an adapter when the engine has no way to express an operation / probe."""
