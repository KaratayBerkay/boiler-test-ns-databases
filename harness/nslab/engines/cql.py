"""CQL adapter: Apache Cassandra 5, ScyllaDB, YugabyteDB (YCQL) — via cassandra-driver.

Wide-column stores are modelled query-first (one table per access path), the way their vendors recommend:
  customers / categories / products / orders / order_items / events   base tables keyed by id (nested objects flattened)
  orders_by_customer (customer_id, id)                                 range_by_secondary / join_lookup
  products_by_category ((category_id), active, price DESC, id)        top_n_sorted
  orders_by_time ((bucket), ordered_at, id)                            pagination (keyset via tuple comparison)
  items_by_order (order_id, id)                                        opt06 referenced items (orders.items keeps a JSON copy)
  inventory ((product_id), warehouse_id) + version                     LWT compare-and-set, single-partition conditional batch
  inventory_qty ((product_id), warehouse_id) qty counter               atomic increment
Queries without a matching table run with ALLOW FILTERING (a full scan) so the cost of a missing access path is visible;
opt01 adds an SAI index (Cassandra 5) or a query table. Nodes are addressed by static container IPs (no host port mapping),
each client is pinned to its target node with a whitelist load-balancing policy.
"""
from __future__ import annotations

import datetime as dt
import json
import math
import re
import time
from pathlib import Path
from typing import Any

from cassandra import ConsistencyLevel, InvalidRequest, OperationTimedOut, Unavailable
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, NoHostAvailable
try:
    from cassandra.io.libevreactor import LibevConnection
except ImportError:  # pragma: no cover
    LibevConnection = None
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import WhiteListRoundRobinPolicy, RetryPolicy
from cassandra.query import UNSET_VALUE, BatchStatement, BatchType, SimpleStatement, tuple_factory

from ..config import Target
from ..datagen import read_docs
from ..ops import Unsupported
from ..util import short_err
from .. import dockerctl
from .base import Client, Engine, EngineError, batched, iso

KS = "lab"

DDL = [
    "CREATE TABLE IF NOT EXISTS customers (id int PRIMARY KEY, name text, email text, country_code text, tier text, created_at timestamp, "
    "attrs_newsletter boolean, attrs_age int, attrs_lang text, lat double, lon double, last_login text)",
    "CREATE TABLE IF NOT EXISTS categories (id int PRIMARY KEY, parent_id int, name text, depth int)",
    "CREATE TABLE IF NOT EXISTS products (id int PRIMARY KEY, category_id int, sku text, name text, description text, price double, "
    "attrs_color text, attrs_weight double, attrs_material text, tags set<text>, active boolean, embedding {VEC})",
    "CREATE TABLE IF NOT EXISTS products_by_category (category_id int, active boolean, price double, id int, "
    "PRIMARY KEY ((category_id), active, price, id)) WITH CLUSTERING ORDER BY (active DESC, price DESC, id ASC)",
    "CREATE TABLE IF NOT EXISTS orders (id int PRIMARY KEY, customer_id int, status text, ordered_at timestamp, total_amount double, "
    "shipping_country text, items text)",
    "CREATE TABLE IF NOT EXISTS orders_by_customer (customer_id int, id int, status text, total_amount double, PRIMARY KEY ((customer_id), id))",
    "CREATE TABLE IF NOT EXISTS orders_by_time (bucket int, ordered_at timestamp, id int, PRIMARY KEY ((bucket), ordered_at, id))",
    "CREATE TABLE IF NOT EXISTS order_items (id int PRIMARY KEY, order_id int, product_id int, qty int, unit_price double)",
    "CREATE TABLE IF NOT EXISTS items_by_order (order_id int, id int, product_id int, qty int, unit_price double, PRIMARY KEY ((order_id), id))",
    "CREATE TABLE IF NOT EXISTS events (id bigint PRIMARY KEY, customer_id int, event_type text, occurred_at timestamp, "
    "payload_page text, payload_ref text, payload_device text, value_num double)",
    "CREATE TABLE IF NOT EXISTS inventory (product_id int, warehouse_id int, qty int, updated_at timestamp, version int, PRIMARY KEY ((product_id), warehouse_id))",
    "CREATE TABLE IF NOT EXISTS inventory_qty (product_id int, warehouse_id int, qty counter, PRIMARY KEY ((product_id), warehouse_id))",
    "CREATE TABLE IF NOT EXISTS repl_probe (id int PRIMARY KEY, ts_ms bigint)",
    "CREATE TABLE IF NOT EXISTS scratch (k text PRIMARY KEY, v text)",
]
TABLES = ["customers", "categories", "products", "products_by_category", "orders", "orders_by_customer", "orders_by_time", "order_items",
          "items_by_order", "events", "inventory", "inventory_qty", "repl_probe", "scratch"]
CL = {"ONE": ConsistencyLevel.ONE, "LOCAL_ONE": ConsistencyLevel.LOCAL_ONE, "LOCAL_QUORUM": ConsistencyLevel.LOCAL_QUORUM,
      "QUORUM": ConsistencyLevel.QUORUM, "ALL": ConsistencyLevel.ALL, "ANY": ConsistencyLevel.ANY}


def _payload(d: dict):
    p = d.get("payload") or {}
    return p.get("page"), p.get("ref"), p.get("device")


class CqlClient(Client):
    def __init__(self, engine: "CqlEngine", target: Target, cluster: Cluster, session):
        super().__init__(engine, target, cluster)
        self.session = session
        self._prepared: dict[str, Any] = {}
        self.cl = engine.default_cl

    def close(self) -> None:
        try:
            self.raw.shutdown()
        except Exception:  # noqa: BLE001
            pass

    def prep(self, cql: str, cl: ConsistencyLevel | None = None):
        key = (cql, cl)
        st = self._prepared.get(key)
        if st is None:
            st = self.session.prepare(cql)
            st.consistency_level = cl if cl is not None else self.cl
            self._prepared[key] = st
        return st

    def ex(self, cql: str, params: tuple = (), cl: ConsistencyLevel | None = None, timeout: float = 120):
        if params and any(v is None for v in params) and cql.lstrip().upper().startswith("INSERT"):
            params = tuple(UNSET_VALUE if v is None else v for v in params)
        return self.session.execute(self.prep(cql, cl), params, timeout=timeout)

    def ex_simple(self, cql: str, cl: ConsistencyLevel | None = None, timeout: float = 120, fetch_size: int | None = None):
        st = SimpleStatement(cql, consistency_level=cl if cl is not None else self.cl, fetch_size=fetch_size)
        return self.session.execute(st, timeout=timeout)

    def many(self, cql: str, params_list: list[tuple], cl: ConsistencyLevel | None = None, concurrency: int = 100) -> None:
        st = self.prep(cql, cl)
        params_list = [tuple(UNSET_VALUE if v is None else v for v in p) for p in params_list]
        res = execute_concurrent_with_args(self.session, st, params_list, concurrency=concurrency, raise_on_first_error=True)
        for ok, r in res:
            if not ok:
                raise r

    # --- lifecycle -----------------------------------------------------------------------------
    def ping(self):
        return self.ex_simple("SELECT release_version FROM system.local", ConsistencyLevel.ONE, timeout=10).one()

    def server_version(self) -> str:
        try:
            r = self.ex_simple("SELECT release_version, cql_version FROM system.local", ConsistencyLevel.ONE, timeout=10).one()
            v = f"Cassandra {r[0]} (CQL {r[1]})"
            if self.engine.flavor == "scylla":
                try:
                    s = self.ex_simple("SELECT version FROM system.versions", ConsistencyLevel.ONE, timeout=10).one()
                    v = f"ScyllaDB {s[0]} (compat Cassandra {r[0]})"
                except Exception:  # noqa: BLE001
                    v = f"ScyllaDB (compat Cassandra {r[0]})"
            elif self.engine.flavor == "yugabyte":
                v = f"YugabyteDB YCQL (compat Cassandra {r[0]})"
            return v
        except Exception as e:  # noqa: BLE001
            return f"unknown ({e})"

    def role(self) -> str:
        return "node"

    # --- primitives --------------------------------------------------------------------------------
    def count(self, coll: str) -> int:
        return int(self.ex_simple(f"SELECT COUNT(*) FROM {coll}", timeout=300).one()[0])

    def get(self, coll: str, key: Any) -> dict | None:
        keycol = "k" if coll == "scratch" else "id"
        r = self.ex(f"SELECT {keycol} FROM {coll} WHERE {keycol} = ?", (key,)).one()
        return {keycol: r[0]} if r else None

    def put(self, coll: str, doc: dict) -> None:
        for cql, params in self._writes(coll, doc):
            self.ex(cql, params)

    def delete(self, coll: str, key: Any) -> None:
        self.ex(f"DELETE FROM {coll} WHERE id = ?", (key,))

    def bulk_put(self, coll: str, docs: list[dict]) -> int:
        groups: dict[str, list[tuple]] = {}
        for d in docs:
            for cql, params in self._writes(coll, d):
                groups.setdefault(cql, []).append(params)
        for cql, plist in groups.items():
            self.many(cql, plist)
        return len(docs)

    def load_bulk_put(self, coll: str, docs: list[dict]) -> int:
        groups: dict[str, list[tuple]] = {}
        for d in docs:
            for cql, params in self._writes(coll, d):
                groups.setdefault(cql, []).append(params)
        for cql, plist in groups.items():
            self.many(cql, plist, cl=ConsistencyLevel.ONE, concurrency=32 if coll == "products" else 100)   # vector index ingestion dislikes high concurrency (5.0 jvector pool limit)
        return len(docs)

    def delete_many(self, coll: str, keys: list[Any]) -> None:
        self.many(f"DELETE FROM {coll} WHERE id = ?", [(k,) for k in keys])

    def drop_collection(self, coll: str) -> None:
        self.ex_simple(f"TRUNCATE {coll}", timeout=300)

    def _writes(self, coll: str, d: dict) -> list[tuple[str, tuple]]:
        if coll == "customers":
            a = d.get("attrs") or {}
            loc = d.get("location") or {}
            return [("INSERT INTO customers (id, name, email, country_code, tier, created_at, attrs_newsletter, attrs_age, attrs_lang, lat, lon) "
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                     (d["id"], d["name"], d["email"], d["country_code"], d["tier"], d["created_at"], a.get("newsletter"), a.get("age"), a.get("lang"),
                      loc.get("lat"), loc.get("lon")))]
        if coll == "categories":
            return [("INSERT INTO categories (id, parent_id, name, depth) VALUES (?, ?, ?, ?)", (d["id"], d.get("parent_id"), d["name"], d["depth"]))]
        if coll == "products":
            a = d.get("attrs") or {}
            emb = d.get("embedding")
            out = [(f"INSERT INTO products (id, category_id, sku, name, description, price, attrs_color, attrs_weight, attrs_material, tags, active{', embedding' if self.engine.vector else ''}) "
                    f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?{', ?' if self.engine.vector else ''})",
                    (d["id"], d["category_id"], d["sku"], d["name"], d["description"], float(d["price"]), a.get("color"), a.get("weight"), a.get("material"),
                     set(a.get("tags") or []), d["active"]) + ((emb,) if self.engine.vector else ())),
                   ("INSERT INTO products_by_category (category_id, active, price, id) VALUES (?, ?, ?, ?)", (d["category_id"], d["active"], float(d["price"]), d["id"]))]
            return out
        if coll == "orders":
            return [("INSERT INTO orders (id, customer_id, status, ordered_at, total_amount, shipping_country, items) VALUES (?, ?, ?, ?, ?, ?, ?)",
                     (d["id"], d["customer_id"], d["status"], d["ordered_at"], float(d["total_amount"]), d["shipping_country"], json.dumps(d.get("items") or []))),
                    ("INSERT INTO orders_by_customer (customer_id, id, status, total_amount) VALUES (?, ?, ?, ?)", (d["customer_id"], d["id"], d["status"], float(d["total_amount"]))),
                    ("INSERT INTO orders_by_time (bucket, ordered_at, id) VALUES (0, ?, ?)", (d["ordered_at"], d["id"]))]
        if coll == "order_items":
            return [("INSERT INTO order_items (id, order_id, product_id, qty, unit_price) VALUES (?, ?, ?, ?, ?)", (d["id"], d["order_id"], d["product_id"], d["qty"], float(d["unit_price"]))),
                    ("INSERT INTO items_by_order (order_id, id, product_id, qty, unit_price) VALUES (?, ?, ?, ?, ?)", (d["order_id"], d["id"], d["product_id"], d["qty"], float(d["unit_price"])))]
        if coll == "events":
            pg, rf, dv = _payload(d)
            return [("INSERT INTO events (id, customer_id, event_type, occurred_at, payload_page, payload_ref, payload_device, value_num) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                     (d["id"], d["customer_id"], d["event_type"], d["occurred_at"], pg, rf, dv, float(d["value_num"])))]
        if coll == "inventory":
            return [("INSERT INTO inventory (product_id, warehouse_id, qty, updated_at, version) VALUES (?, ?, ?, ?, ?)",
                     (d["product_id"], d["warehouse_id"], d["qty"], d["updated_at"], d.get("version", 1))),
                    ("UPDATE inventory_qty SET qty = qty + ? WHERE product_id = ? AND warehouse_id = ?", (d["qty"], d["product_id"], d["warehouse_id"]))]
        if coll == "repl_probe":
            return [("INSERT INTO repl_probe (id, ts_ms) VALUES (?, ?)", (d["id"], d["ts_ms"]))]
        if coll == "scratch":
            return [("INSERT INTO scratch (k, v) VALUES (?, ?)", (d["k"], d["v"]))]
        raise Unsupported(f"no table for {coll}")

    # --- operations -------------------------------------------------------------------------------------
    def op_get_by_key(self, p):
        r = self.ex("SELECT id, name, email, country_code FROM customers WHERE id = ?", (p["id"],)).one()
        return [tuple(r)] if r else []

    def op_multi_get(self, p):
        # 100 concurrent single-partition reads (the driver-recommended pattern; a 100-key IN list makes the
        # coordinator fetch the partitions one after another: ~70 ms vs ~5 ms here)
        st = self.prep("SELECT id, tier FROM customers WHERE id = ?")
        res = execute_concurrent_with_args(self.session, st, [(i,) for i in p["ids"]], concurrency=100, results_generator=False)
        out = []
        for ok, rows in res:
            if not ok:
                raise rows
            r = rows.one()
            if r:
                out.append((r[0], r[1]))
        return sorted(out)

    def op_insert_one(self, p):
        self.put("events", p)
        return None

    def op_update_field(self, p):
        self.ex("UPDATE customers SET last_login = ? WHERE id = ?", (p["value"], p["id"]))
        return None

    def op_upsert(self, p):
        self.ex("INSERT INTO inventory (product_id, warehouse_id, qty, updated_at, version) VALUES (?, ?, ?, ?, 1)",
                (p["product_id"], p["warehouse_id"], p["qty"], dt.datetime.utcnow()))
        return None

    def op_delete_one(self, p):
        self.ex("DELETE FROM events WHERE id = ?", (p["id"],))
        return None

    def op_atomic_increment(self, p):
        self.ex("UPDATE inventory_qty SET qty = qty + 1 WHERE product_id = ? AND warehouse_id = ?", (p["product_id"], p["warehouse_id"]))
        r = self.ex("SELECT qty FROM inventory_qty WHERE product_id = ? AND warehouse_id = ?", (p["product_id"], p["warehouse_id"])).one()
        return [(int(r[0]),)] if r else []

    def op_conditional_update(self, p):
        r = self.ex("SELECT version FROM inventory WHERE product_id = ? AND warehouse_id = ?", (p["product_id"], p["warehouse_id"])).one()
        if not r:
            return [(0, -1)]
        v = r[0]
        res = self.ex("UPDATE inventory SET qty = 250, version = ? WHERE product_id = ? AND warehouse_id = ? IF version = ?",
                      (v + 1, p["product_id"], p["warehouse_id"], v)).one()
        applied = bool(res[0])
        return [(1 if applied else 0, v + 1 if applied else (res[1] if len(res) > 1 and res[1] is not None else v))]

    def op_batch_insert_1000(self, p):
        self.bulk_put("events", p["docs"])
        return None

    def op_range_by_secondary(self, p):
        rows = self.ex("SELECT id, status, total_amount FROM orders_by_customer WHERE customer_id = ?", (p["customer_id"],))
        return [(r[0], r[1], r[2]) for r in rows]

    def op_range_by_time(self, p):
        if self.engine.range_index == "sai":
            st = SimpleStatement("SELECT id, customer_id, occurred_at FROM events WHERE event_type = %s AND occurred_at >= %s AND occurred_at < %s",
                                 consistency_level=self.cl, fetch_size=5000)
            rows = self.session.execute(st, (p["event_type"], p["start"], p["end"]), timeout=300)
        elif self.engine.range_index == "table":
            months = sorted({p["start"].strftime("%Y-%m"), p["end"].strftime("%Y-%m")})
            rows = []
            for m in months:
                rows += list(self.ex("SELECT id, customer_id, occurred_at FROM events_by_type_month WHERE event_type = ? AND month = ? AND occurred_at >= ? AND occurred_at < ?",
                                     (p["event_type"], m, p["start"], p["end"])))
        else:
            st = SimpleStatement("SELECT id, customer_id, occurred_at FROM events WHERE event_type = %s AND occurred_at >= %s AND occurred_at < %s ALLOW FILTERING",
                                 consistency_level=self.cl, fetch_size=5000)
            rows = self.session.execute(st, (p["event_type"], p["start"], p["end"]), timeout=300)
        out = sorted(((r[0], r[1], iso(r[2].replace(tzinfo=None) if r[2].tzinfo else r[2])) for r in rows), key=lambda t: (t[2], t[0]))
        return out[:p["limit"]]

    def op_filter_nested(self, p):
        r = self.ex_simple("SELECT COUNT(*) FROM customers WHERE tier = 'pro' AND attrs_lang = 'tr' ALLOW FILTERING", timeout=300).one()
        return [(int(r[0]),)]

    def op_array_contains(self, p):
        r = self.ex_simple("SELECT COUNT(*) FROM products WHERE tags CONTAINS 'eco' AND active = true ALLOW FILTERING", timeout=300).one()
        return [(int(r[0]),)]

    def op_top_n_sorted(self, p):
        rows = self.ex("SELECT id, price FROM products_by_category WHERE category_id = ? AND active = true LIMIT ?", (p["category_id"], p["n"]))
        return [(r[0], r[1]) for r in rows]

    def op_pagination_offset(self, p):
        # CQL has no OFFSET: page through the partition and discard (what an application must do)
        st = SimpleStatement("SELECT id, ordered_at FROM orders_by_time WHERE bucket = 0", consistency_level=self.cl, fetch_size=5000)
        out, skipped = [], 0
        for r in self.session.execute(st, timeout=300):
            if skipped < p["offset"]:
                skipped += 1
                continue
            out.append((r[0], iso(r[1].replace(tzinfo=None))))
            if len(out) >= p["page"]:
                break
        return out

    def op_pagination_keyset(self, p):
        cur = getattr(self, "_cursor", None)
        if cur is None:
            # find the cursor once (paging to the offset), then every page is a clustering-range seek
            st = SimpleStatement("SELECT id, ordered_at FROM orders_by_time WHERE bucket = 0", consistency_level=self.cl, fetch_size=5000)
            n = 0
            for r in self.session.execute(st, timeout=300):
                n += 1
                if n == p["offset"]:
                    cur = self._cursor = (r[1], r[0])
                    break
        rows = self.ex("SELECT id, ordered_at FROM orders_by_time WHERE bucket = 0 AND (ordered_at, id) > (?, ?) LIMIT ?", (cur[0], cur[1], p["page"]))
        return [(r[0], iso(r[1].replace(tzinfo=None))) for r in rows]

    def op_count_filtered(self, p):
        r = self.ex_simple("SELECT COUNT(*) FROM orders WHERE status = 'pending' ALLOW FILTERING", timeout=300).one()
        return [(int(r[0]),)]

    def op_aggregate_group(self, p):
        st = SimpleStatement("SELECT shipping_country, total_amount FROM orders", consistency_level=self.cl, fetch_size=10000)
        agg: dict[str, list] = {}
        for r in self.session.execute(st, timeout=300):
            a = agg.setdefault(r[0], [0, 0.0])
            a[0] += 1
            a[1] += r[1]
        return [(c, n, round(s, 2)) for c, (n, s) in sorted(agg.items())]

    def op_aggregate_time_bucket(self, p):
        st = SimpleStatement("SELECT occurred_at, value_num FROM events WHERE event_type = 'checkout' ALLOW FILTERING", consistency_level=self.cl, fetch_size=10000)
        agg: dict[str, list] = {}
        for r in self.session.execute(st, timeout=600):
            k = r[0].strftime("%Y-%m")
            a = agg.setdefault(k, [0, 0.0])
            a[0] += 1
            a[1] += r[1]
        return [(m, n, round(s, 4)) for m, (n, s) in sorted(agg.items())]

    def op_join_lookup(self, p):
        name = self.ex("SELECT name FROM customers WHERE id = ?", (p["customer_id"],)).one()[0]
        rows = self.ex("SELECT id, total_amount FROM orders_by_customer WHERE customer_id = ?", (p["customer_id"],))
        return [(r[0], name, r[1]) for r in rows]

    def op_graph_traversal(self, p):
        cat = self.ex("SELECT category_id FROM products WHERE id = ?", (p["product_id"],)).one()[0]
        path = []
        while cat is not None and len(path) < 10:
            r = self.ex("SELECT depth, parent_id FROM categories WHERE id = ?", (cat,)).one()
            path.append((r[0], cat))
            cat = r[1]
        return sorted(path)

    def op_text_search(self, p):
        if self.engine.text_index:
            try:
                q = " AND ".join(f"description : '{w}'" for w in p["words"])
                rows = self.ex_simple(f"SELECT id FROM products WHERE {q} LIMIT 1000", timeout=60)
                return [(i,) for i in sorted(r[0] for r in rows)[:p["limit"]]]
            except InvalidRequest as e:
                raise Unsupported(f"analyzed SAI text index: {str(e)[:100]}")
        # no text index: scan the 5k product descriptions client-side
        st = SimpleStatement("SELECT id, description FROM products", consistency_level=self.cl, fetch_size=5000)
        words = [w.lower() for w in p["words"]]
        out = [r[0] for r in self.session.execute(st, timeout=120) if r[1] and all(w in r[1] for w in words)]
        return [(i,) for i in sorted(out)[:p["limit"]]]

    def op_geo_near(self, p):
        # no geo index: haversine over all customers (client-side scan)
        st = SimpleStatement("SELECT id, lat, lon FROM customers", consistency_level=self.cl, fetch_size=5000)
        lat0, lon0 = math.radians(p["lat"]), math.radians(p["lon"])
        hits = []
        for r in self.session.execute(st, timeout=120):
            if r[1] is None:
                continue
            la, lo = math.radians(r[1]), math.radians(r[2])
            h = math.sin((la - lat0) / 2) ** 2 + math.cos(lat0) * math.cos(la) * math.sin((lo - lon0) / 2) ** 2
            d = 2 * 6371.0 * math.asin(math.sqrt(h))
            if d <= p["km"]:
                hits.append((d, r[0]))
        hits.sort()
        return [(i,) for _, i in hits[:p["limit"]]]

    def op_vector_knn(self, p):
        if not self.engine.vector:
            raise Unsupported("no vector type / ANN index in this engine")
        rows = self.ex("SELECT id FROM products ORDER BY embedding ANN OF ? LIMIT ?", (p["vector"], p["k"]))
        return [(r[0],) for r in rows]

    def op_transaction_transfer(self, p):
        # single-partition conditional (LWT) batch: both rows share the product_id partition
        rows = {r[0]: r[1] for r in self.ex("SELECT warehouse_id, qty FROM inventory WHERE product_id = ?", (p["product_id"],))}
        qa, qb = rows.get(p["from"]), rows.get(p["to"])
        if qa is None or qb is None:
            raise RuntimeError("inventory rows missing")
        b = BatchStatement(batch_type=BatchType.LOGGED, consistency_level=self.cl)
        b.add(self.prep("UPDATE inventory SET qty = ? WHERE product_id = ? AND warehouse_id = ? IF qty = ?"), (qa - p["amount"], p["product_id"], p["from"], qa))
        b.add(self.prep("UPDATE inventory SET qty = ? WHERE product_id = ? AND warehouse_id = ? IF qty = ?"), (qb + p["amount"], p["product_id"], p["to"], qb))
        r = self.session.execute(b, timeout=60).one()
        if r is not None and r[0] is False:
            raise RuntimeError("conditional batch not applied (concurrent change)")
        return None

    def op_ttl_set(self, p):
        self.ex("INSERT INTO scratch (k, v) VALUES (?, ?) USING TTL ?", (p["key"], json.dumps(p["value"]), p["seconds"]))
        return None

    def op_scan_full(self, p):
        return [(self.count("events"),)]

    # --- optimisation hooks -----------------------------------------------------------------------------
    def opt_fetch_full(self, coll, key):
        r = self.ex(f"SELECT * FROM {coll} WHERE id = ?", (key,)).one()
        return [tuple(r)] if r else []

    def opt_fetch_projected(self, coll, key, fields):
        r = self.ex(f"SELECT {', '.join(fields)} FROM {coll} WHERE id = ?", (key,)).one()
        return [tuple(r)] if r else []

    def opt_order_embedded(self, order_id):
        r = self.ex("SELECT id, items FROM orders WHERE id = ?", (order_id,)).one()
        return [(r[0], len(json.loads(r[1] or "[]")))]

    def opt_order_referenced(self, order_id):
        r = self.ex("SELECT id, status, total_amount FROM orders WHERE id = ?", (order_id,)).one()
        items = list(self.ex("SELECT id, product_id, qty FROM items_by_order WHERE order_id = ?", (order_id,)))
        return [(r[0], len(items))]

    def opt_write_durable(self, level, doc):
        pg, rf, dv = _payload(doc)
        self.ex("INSERT INTO events (id, customer_id, event_type, occurred_at, payload_page, payload_ref, payload_device, value_num) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (doc["id"], doc["customer_id"], doc["event_type"], doc["occurred_at"], pg, rf, dv, float(doc["value_num"])), cl=CL[level])
        return None

    def opt_read_level(self, level, key):
        r = self.ex("SELECT id, name FROM customers WHERE id = ?", (key,), cl=CL[level]).one()
        return [tuple(r)] if r else []

    # --- probes -----------------------------------------------------------------------------------------------
    STATIC_PROBES = {
        "bulk_import": ("supported", "COPY FROM (cqlsh), sstableloader, DSBulk / concurrent prepared statements (driver)"),
        "nested_query": ("unsupported", "no nested documents: UDTs/collections are opaque to WHERE except CONTAINS; flatten or use SAI on UDT fields"),
        "aggregation": ("unsupported", "GROUP BY only on partition/clustering key prefix; COUNT/SUM/AVG over one partition or with ALLOW FILTERING (full scan)"),
        "server_side_join": ("unsupported", "no joins: denormalise (one table per query) or join client-side / in Spark"),
        "graph_traversal": ("unsupported", "client-side"),
        "pubsub": ("unsupported", "no pub/sub (CDC log tables for change capture)"),
    }

    def probe_secondary_index(self):
        idx = "USING 'sai'" if self.engine.flavor == "cassandra" else ""
        self.ex_simple(f"CREATE INDEX IF NOT EXISTS orders_status_idx ON orders (status) {idx}", timeout=120)
        self.engine.wait_index(self, "orders_status_idx")
        try:
            rows = list(self.ex_simple("SELECT id FROM orders WHERE status = 'pending' LIMIT 1000", timeout=120))
        finally:
            self.ex_simple("DROP INDEX IF EXISTS orders_status_idx", timeout=120)
        return f"{'SAI' if idx else 'secondary'} index on orders(status): {len(rows)} rows fetched via the index (global index; Scylla: materialized-view based)"

    def probe_compound_index(self):
        raise Unsupported("no multi-column index; compound access paths are modelled as (partition key, clustering columns) of a query table")

    def probe_unique_constraint(self):
        r = self.ex("INSERT INTO scratch (k, v) VALUES (?, ?) IF NOT EXISTS", ("uniq", "1")).one()
        r2 = self.ex("INSERT INTO scratch (k, v) VALUES (?, ?) IF NOT EXISTS", ("uniq", "2")).one()
        self.ex("DELETE FROM scratch WHERE k = ?", ("uniq",))
        if r2[0]:
            raise RuntimeError("second INSERT IF NOT EXISTS applied")
        return "INSERT ... IF NOT EXISTS (LWT) on the primary key only; no unique constraint on other columns"

    def probe_ttl_expiry(self):
        self.ex("INSERT INTO scratch (k, v) VALUES (?, ?) USING TTL 1", ("ttl", "1"))
        time.sleep(1.5)
        if self.ex("SELECT v FROM scratch WHERE k = ?", ("ttl",)).one():
            raise RuntimeError("row still present after TTL")
        return "USING TTL per write / default_time_to_live per table"

    def probe_text_index(self):
        if not self.engine.text_index:
            raise Unsupported("no analyzed text index (Cassandra 5 SAI has one; Scylla/YCQL do not)")
        self.op_text_search({"words": ["wireless"], "limit": 1})
        return "SAI with index_analyzer (StandardAnalyzer) + `column : 'term'` predicate"

    def probe_geo_index(self):
        raise Unsupported("no geospatial type / index")

    def probe_vector_index(self):
        if not self.engine.vector:
            raise Unsupported("no vector type (Cassandra 5 has vector<float, n> + SAI ANN)")
        self.op_vector_knn({"vector": [0.1] * 8, "k": 1})
        return "vector<float, 8> column + SAI index + ORDER BY ... ANN OF"

    def probe_partial_update(self):
        self.ex("UPDATE customers SET last_login = ? WHERE id = 1", ("probe",))
        return "UPDATE SET column = ? (cell-level upsert)"

    def probe_atomic_increment(self):
        return f"counter column: UPDATE ... SET qty = qty + 1 -> {self.op_atomic_increment({'product_id': 1, 'warehouse_id': 1})}"

    def probe_optimistic_concurrency(self):
        return f"lightweight transaction (Paxos / Raft): UPDATE ... IF version = ? -> {self.op_conditional_update({'product_id': 1, 'warehouse_id': 1})}"

    def probe_multi_doc_transaction(self):
        self.op_transaction_transfer({"product_id": 1, "from": 1, "to": 2, "amount": 0})
        return "conditional BATCH within one partition (atomic, isolated); LOGGED BATCH across partitions is atomic but not isolated; no rollback API"

    def probe_tunable_consistency(self):
        self.ex("INSERT INTO scratch (k, v) VALUES (?, ?)", ("cl", "1"), cl=ConsistencyLevel.ALL)
        self.ex("SELECT v FROM scratch WHERE k = ?", ("cl",), cl=ConsistencyLevel.ONE)
        return "per-statement consistency level: ANY/ONE/QUORUM/LOCAL_QUORUM/EACH_QUORUM/ALL (+ SERIAL for LWT)"

    def probe_array_operators(self):
        self.ex("UPDATE products SET tags = tags + {'probe'} WHERE id = 1", ())
        r = self.ex_simple("SELECT COUNT(*) FROM products WHERE tags CONTAINS 'probe' ALLOW FILTERING", timeout=60).one()
        self.ex("UPDATE products SET tags = tags - {'probe'} WHERE id = 1", ())
        return f"set/list/map collections with += / -= updates, CONTAINS (index or ALLOW FILTERING) -> {r[0]}"

    def probe_schema_validation(self):
        try:
            self.ex("INSERT INTO categories (id, parent_id, name, depth) VALUES (?, ?, ?, ?)", (1, 1, "x", "notanint"))
        except Exception:  # noqa: BLE001
            return "typed columns enforced by the schema (no constraints beyond types/keys)"
        raise RuntimeError("type mismatch accepted")

    def probe_change_streams(self):
        if self.engine.flavor == "yugabyte":
            raise Unsupported("YCQL: CDC via yb-admin create_change_data_stream + Debezium, not a CQL statement")
        on, off = ("{'enabled': true}", "{'enabled': false}") if self.engine.flavor == "scylla" else ("true", "false")
        self.ex_simple(f"ALTER TABLE scratch WITH cdc = {on}", timeout=60)
        self.ex_simple(f"ALTER TABLE scratch WITH cdc = {off}", timeout=60)
        return "CDC (Cassandra: commit-log segments read by an external agent; Scylla: CDC log table queryable with CQL)"

    def probe_server_side_scripting(self):
        try:
            self.ex_simple("CREATE OR REPLACE FUNCTION lab.dbl (x int) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return x * 2;'", timeout=60)
            r = self.ex_simple("SELECT lab.dbl(depth) FROM categories WHERE id = 1", timeout=60).one()
            return f"user-defined functions (Java/Lua; disabled by default: user_defined_functions_enabled) -> {r[0]}"
        except Exception as e:  # noqa: BLE001
            raise Unsupported(f"UDFs: {str(e)[:100]}")

    def probe_explain_plan(self):
        try:
            self.ex_simple("TRACING ON", timeout=10)
        except Exception:  # noqa: BLE001
            pass
        st = SimpleStatement("SELECT id FROM orders WHERE id = 1")
        rs = self.session.execute(st, trace=True, timeout=60)
        tr = rs.get_query_trace(max_wait_sec=5)
        return f"no EXPLAIN; request tracing: {len(tr.events) if tr else 0} events (coordinator: {tr.coordinator if tr else '?'})"

    def probe_large_document(self):
        blob = "x" * (15 * 1024 * 1024)
        self.ex("INSERT INTO scratch (k, v) VALUES (?, ?)", ("big", blob))
        n = len(self.ex("SELECT v FROM scratch WHERE k = ?", ("big",)).one()[0])
        self.ex("DELETE FROM scratch WHERE k = ?", ("big",))
        return f"{n/1048576:.0f} MB text cell ok (max_mutation_size default 16 MB = half commitlog_segment_size)"

    def probe_many_fields(self):
        raise Unsupported("columns are schema-defined (5000 ALTER TABLE ADD is possible but pathological); use a map<text,text> for sparse attributes")

    def probe_read_from_replica(self):
        others = [t for t in self.cfg.targets if t is not self.target]
        if not others:
            raise Unsupported("single node")
        c = self.engine.connect(others[0])
        try:
            c.ex("SELECT name FROM customers WHERE id = 1", (), cl=ConsistencyLevel.ONE)
            return "leaderless: any replica serves reads (consistency level ONE); driver load balancing spreads requests"
        finally:
            c.close()

    def probe_snapshot_backup(self):
        if not self.target.container:
            raise Unsupported("nodetool snapshot (per node, hard links) — a JMX/nodetool command, not CQL; Scylla: nodetool snapshot / Scylla Manager")
        rc, out, err = dockerctl.exec_in(self.target.container, ["nodetool", "listsnapshots"], timeout=120)
        if rc != 0:
            raise EngineError(err[:200])
        return "nodetool snapshot (per-node hard links, --ttl expiry) + listsnapshots/clearsnapshot; online, instant (exercised in the backup phase)"

    def probe_slow_query_log(self):
        return "slow_query_log_timeout (500 ms default, cassandra.yaml) -> MonitoringTask aggregates slow reads into debug.log every 5 s"

    def probe_audit_log(self):
        if not self.target.container:
            raise Unsupported("nodetool enableauditlog needs node access")
        rc, out, err = dockerctl.exec_in(self.target.container, ["nodetool", "getauditlog"], timeout=120)
        if rc != 0:
            raise EngineError(err[:200])
        return "nodetool enableauditlog (BinAuditLogger -> logs/audit, auditlogviewer) / full query log (enablefullquerylog + fqltool)"

    def probe_runtime_log_level(self):
        if not self.target.container:
            raise Unsupported("nodetool setlogginglevel needs node access")
        rc, out, err = dockerctl.exec_in(self.target.container, ["nodetool", "getlogginglevels"], timeout=120)
        if rc != 0:
            raise EngineError(err[:200])
        return "nodetool setlogginglevel <logger> <level> (logback, per node, no restart)"

    STATIC_PROBES = dict(STATIC_PROBES, **{
        "structured_logs": ("unsupported", "logback text (system.log / debug.log / gc.log); JSON needs a logback JSON encoder on the classpath"),
        "incremental_backup": ("supported", "incremental_backups / nodetool enablebackup: every flushed SSTable is hard-linked into <table>/backups/"),
        "pitr": ("supported", "commitlog archiving (commitlog_archiving.properties: archive_command + restore_point_in_time) — not exercised here"),
    })


class CqlEngine(Engine):
    driver = "cql"
    load_workers = 8
    load_batch = 2000
    load_method = "execute_concurrent (prepared, 256 in flight, CL=ONE)"

    def apply_features(self, features: dict) -> None:
        self.flavor = features.get("flavor", "cassandra")          # cassandra | scylla | yugabyte
        self.vector = bool(features.get("vector", self.flavor == "cassandra"))
        self.text_index = bool(features.get("text_index", self.flavor == "cassandra"))
        self.range_index = None                                     # None | 'sai' | 'table' (set by opt01)
        self.default_cl = CL[features.get("consistency", "LOCAL_QUORUM")]
        self.rf = int(features.get("replication_factor", 3))
        self.keyspace_options = features.get("keyspace_options", "")

    def connect(self, target: Target, *, timeout: float = 10.0) -> CqlClient:
        profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([target.host]), request_timeout=120,
                                   consistency_level=self.default_cl, row_factory=tuple_factory)
        kw: dict[str, Any] = {}
        if LibevConnection is not None:
            kw["connection_class"] = LibevConnection
        if self.flavor != "cassandra":
            kw["protocol_version"] = 4              # Scylla / YCQL: skip the v5/v6 negotiation round trips
        cluster = Cluster(contact_points=[target.host], port=target.port or 9042, execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                          connect_timeout=timeout, control_connection_timeout=timeout, idle_heartbeat_interval=30, **kw)
        try:
            session = cluster.connect()
        except NoHostAvailable as e:
            cluster.shutdown()
            raise EngineError(f"connect {target.describe()}: {str(e)[:200]}") from e
        try:
            session.set_keyspace(KS)
        except InvalidRequest:
            pass
        return CqlClient(self, target, cluster, session)

    def collections(self) -> list[str]:
        from ..schema import LOAD_ORDER
        return list(LOAD_ORDER)

    def reset(self, client: CqlClient) -> None:
        client.session.execute(f"DROP KEYSPACE IF EXISTS {KS}", timeout=300)
        repl = "{'class': 'NetworkTopologyStrategy', 'replication_factor': %d}" % self.rf if self.flavor != "yugabyte" else "{'class': 'SimpleStrategy', 'replication_factor': %d}" % self.rf
        client.session.execute(f"CREATE KEYSPACE {KS} WITH replication = {repl} {self.keyspace_options}", timeout=300)
        client.session.set_keyspace(KS)

    def create_schema(self, client: CqlClient) -> list[str]:
        done = []
        vec = "vector<float, 8>" if self.vector else "text"
        for ddl in DDL:
            d = ddl.replace("{VEC}", vec)
            client.session.execute(d, timeout=300)
            done.append(d.split("(")[0].strip())
        if self.vector:
            client.session.execute("CREATE CUSTOM INDEX IF NOT EXISTS products_emb_idx ON products (embedding) USING 'StorageAttachedIndex' "
                                   "WITH OPTIONS = {'similarity_function': 'cosine'}", timeout=300)
            done.append("SAI vector index products(embedding) cosine")
        if self.text_index:
            try:
                client.session.execute("CREATE CUSTOM INDEX IF NOT EXISTS products_desc_idx ON products (description) USING 'StorageAttachedIndex' "
                                       "WITH OPTIONS = {'index_analyzer': 'standard'}", timeout=300)
                done.append("SAI analyzed text index products(description)")
            except Exception as e:  # noqa: BLE001
                self.text_index = False
                done.append(f"text index FAILED: {str(e)[:120]}")
        return done

    def after_load(self, client: CqlClient) -> list[str]:
        time.sleep(2)
        return ["(memtables flush on their own; no statistics step)"]

    def index_variants(self) -> list[tuple[str, Any]]:
        if self.flavor == "cassandra":
            return [("no_index_allow_filtering", None), ("sai_index_(event_type,occurred_at)", "sai"), ("query_table_((event_type,month),occurred_at)", "table")]
        return [("no_index_allow_filtering", None), ("secondary_index_event_type", "sai"), ("query_table_((event_type,month),occurred_at)", "table")]

    def create_index(self, client: CqlClient, name: str, coll: str, spec: Any) -> str:
        if spec == "sai":
            if self.flavor == "cassandra":
                client.session.execute("CREATE INDEX IF NOT EXISTS ev_type_sai ON events (event_type) USING 'sai'", timeout=600)
                client.session.execute("CREATE INDEX IF NOT EXISTS ev_ts_sai ON events (occurred_at) USING 'sai'", timeout=600)
                self._wait_index_build(client)
                self.range_index = "sai"
                return "SAI indexes on events(event_type) and events(occurred_at) (query ANDs both, no ALLOW FILTERING)"
            client.session.execute("CREATE INDEX IF NOT EXISTS ev_type_idx ON events (event_type)", timeout=600)
            self._wait_index_build(client)
            self.range_index = "sai"
            return "global secondary index on events(event_type); occurred_at range filtered within the index result"
        if spec == "table":
            client.session.execute("CREATE TABLE IF NOT EXISTS events_by_type_month (event_type text, month text, occurred_at timestamp, id bigint, customer_id int, "
                                   "PRIMARY KEY ((event_type, month), occurred_at, id))", timeout=300)
            st = SimpleStatement("SELECT id, customer_id, event_type, occurred_at FROM events", consistency_level=ConsistencyLevel.ONE, fetch_size=10000)
            ins = client.prep("INSERT INTO events_by_type_month (event_type, month, occurred_at, id, customer_id) VALUES (?, ?, ?, ?, ?)", ConsistencyLevel.ONE)
            n = 0
            buf = []
            for r in client.session.execute(st, timeout=1200):
                buf.append((r[2], r[3].strftime("%Y-%m"), r[3], r[0], r[1]))
                if len(buf) >= 5000:
                    execute_concurrent_with_args(client.session, ins, buf, concurrency=100)
                    n += len(buf)
                    buf = []
            if buf:
                execute_concurrent_with_args(client.session, ins, buf, concurrency=100)
                n += len(buf)
            self.range_index = "table"
            return f"denormalised query table events_by_type_month populated with {n} rows"
        raise Unsupported("unknown index spec")

    def wait_index(self, client: CqlClient, name: str, timeout: float = 300) -> None:
        t0 = time.time()
        while time.time() - t0 < timeout:
            try:
                if self.flavor == "cassandra":
                    built = {r[1] for r in client.session.execute("SELECT * FROM system.\"IndexInfo\"", timeout=30)}
                    if name in built:
                        return
                else:
                    time.sleep(5)
                    return
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.5)

    def _wait_index_build(self, client: CqlClient) -> None:
        for _ in range(600):
            try:
                if self.flavor == "cassandra":
                    rows = list(client.session.execute("SELECT * FROM system.\"IndexInfo\"", timeout=30))
                    built = {r[1] for r in rows}
                    if {"ev_type_sai", "ev_ts_sai"} <= built:
                        return
                else:
                    rows = list(client.session.execute("SELECT index_name FROM system_schema.indexes WHERE keyspace_name = 'lab' ALLOW FILTERING", timeout=30))
                    time.sleep(10)
                    return
            except Exception:  # noqa: BLE001
                pass
            time.sleep(1)

    def drop_index(self, client: CqlClient, name: str, coll: str) -> None:
        for stmt in ("DROP INDEX IF EXISTS ev_type_sai", "DROP INDEX IF EXISTS ev_ts_sai", "DROP INDEX IF EXISTS ev_type_idx", "DROP TABLE IF EXISTS events_by_type_month"):
            try:
                client.session.execute(stmt, timeout=300)
            except Exception:  # noqa: BLE001
                pass
        self.range_index = None

    def durability_levels(self) -> list[str]:
        return ["ONE", "LOCAL_QUORUM", "ALL"]

    def read_levels(self) -> list[tuple[str, str | None]]:
        return [("ONE", None), ("LOCAL_QUORUM", None), ("ALL", None)]

    def max_connections(self, client: CqlClient) -> int | None:
        return None

    def replication_status(self, client: CqlClient) -> dict[str, Any]:
        try:
            rows = client.ex_simple("SELECT peer, rpc_address, schema_version FROM system.peers", ConsistencyLevel.ONE, timeout=10)
            peers = [str(r[0]) for r in rows]
            local = client.ex_simple("SELECT rpc_address, tokens FROM system.local", ConsistencyLevel.ONE, timeout=10).one()
            return {"local": str(local[0]), "peers": peers, "tokens": len(local[1]) if local[1] else None}
        except Exception as e:  # noqa: BLE001
            return {"error": str(e)[:100]}

    # --- backup / restore ------------------------------------------------------------------------------
    BACKUP_STRATEGY = {
        "tool": "nodetool flush + nodetool snapshot -t <tag> --ttl 3h (hard-linked SSTables, instant, per node) then export the snapshot dirs to /backups/<tag>/<node>/lab/<table>/",
        "consistency": "per-node crash-consistent after the flush; cluster-wide consistency comes from repair after restore (or a coordinated snapshot)",
        "restore": "online: TRUNCATE every table, then each node runs nodetool import --copy-data on its own exported snapshot (server-side, SAI components included; without --copy-data the export is moved, i.e. consumed); sstableloader streams to the owning replicas but its driver cannot parse vector<float, 8> on 5.0.x",
        "incremental": "incremental_backups (nodetool enablebackup): each flushed SSTable is hard-linked into <table>/backups/ — ship those as they appear",
        "pitr": "commitlog archiving (archive_command + restore_point_in_time) — configured in commitlog_archiving.properties",
        "schedule": "cron on every node (nodetool snapshot) or Medusa / Scylla Manager; --ttl makes snapshots self-expiring",
        "retention": "nodetool clearsnapshot / snapshot --ttl; hard links cost nothing until compaction rewrites the SSTables",
    }

    def _node_containers(self) -> list[Target]:
        return [t for t in self.cfg.targets if t.container]

    def data_bytes(self, client: CqlClient) -> int | None:
        per = {}
        for t in self._node_containers():          # live SSTables only: snapshots/ (auto_snapshot on TRUNCATE) and backups/ are hard links
            rc, out, _ = dockerctl.exec_in(t.container, ["du", "-sb", "--exclude=snapshots", "--exclude=backups", f"/var/lib/cassandra/data/{KS}"], user="0", timeout=600)
            per[t.name] = int(out.split()[0]) if rc == 0 and out.strip() else 0
        self._data_per_node = per
        return sum(per.values()) or None

    def backup(self, client: CqlClient, tag: str, *, log=print) -> dict[str, Any]:
        nodes = self._node_containers()
        if not nodes:
            raise Unsupported("no node containers (managed service): use the provider's snapshot API")
        per_node: dict[str, Any] = {}
        for t in nodes:
            t0 = time.perf_counter()
            rc, out, err = dockerctl.exec_in(t.container, ["nodetool", "flush", KS], timeout=600)
            if rc != 0:
                raise EngineError(f"{t.name} nodetool flush: {err[-200:]}")
            rc, out, err = dockerctl.exec_in(t.container, ["nodetool", "snapshot", "-t", tag, "--ttl", "3h", KS], timeout=600)
            if rc != 0:
                raise EngineError(f"{t.name} nodetool snapshot: {err[-200:]}")
            snap_ms = round((time.perf_counter() - t0) * 1000, 1)
            # export: <data>/lab/<table>-<uuid>/snapshots/<tag>/*  ->  /backups/<tag>/<node>/lab/<table>/  (sstableloader layout)
            script = (f'set -e; for d in /var/lib/cassandra/data/{KS}/*/snapshots/{tag}; do tb=$(basename $(dirname $(dirname $d))); tb=${{tb%-*}}; '
                      f'mkdir -p /backups/{tag}/{t.name}/{KS}/$tb; cp -a "$d"/. /backups/{tag}/{t.name}/{KS}/$tb/; done; '
                      f'du -sb /backups/{tag}/{t.name} | cut -f1')
            t1 = time.perf_counter()
            rc, out, err = dockerctl.exec_in(t.container, ["sh", "-c", script], user="0", timeout=1800)
            if rc != 0:
                raise EngineError(f"{t.name} export snapshot: {err[-300:]}")
            per_node[t.name] = {"snapshot_ms": snap_ms, "export_ms": round((time.perf_counter() - t1) * 1000, 1), "bytes": int(out.strip() or 0)}
        rc, out, err = dockerctl.exec_in(nodes[0].container, ["nodetool", "listsnapshots"], timeout=120)
        listing = [ln for ln in out.splitlines() if tag in ln][:3]
        return {"method": "nodetool flush + snapshot --ttl 3h on every node, export to /backups (sstableloader layout)", "online": True,
                "artifact": f"/backups/{tag}/<node>/{KS}/<table>/", "artifact_bytes": sum(v["bytes"] for v in per_node.values()),
                "per_node": per_node, "listsnapshots_sample": listing,
                "note": f"RF={self.rf} on {len(nodes)} nodes: each node's export is a full copy of the keyspace; every node re-imports its own"}

    def artifact_paths(self, tag: str) -> list[tuple[str, str]]:
        return [(self._node_containers()[0].container, f"/backups/{tag}")]      # every node's export sits in the shared volume

    def _keyspace_exists(self, client: CqlClient) -> bool:
        r = client.session.execute(f"SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = '{KS}'", timeout=30).one()
        return r is not None

    def _tables(self, client: CqlClient) -> list[str]:
        rows = client.session.execute(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{KS}'", timeout=60)
        return sorted(r[0] for r in rows)

    def restore(self, client: CqlClient, tag: str, info: dict[str, Any], *, log=print) -> dict[str, Any]:
        """TRUNCATE every table, then every node imports its own exported snapshot with `nodetool import --copy-data` (server-side,
        online, keeps SAI components). sstableloader would stream one export to all replicas, but its bundled driver cannot
        parse `vector<float, 8>` in 5.0.x, so the vector table fails — nodetool import is the path that works on 5.0."""
        import threading
        nodes = self._node_containers()
        schema_created = False
        if not self._keyspace_exists(client):                 # fresh cluster (after down -v): recreate keyspace + tables first
            self.reset(client)
            self.create_schema(client)
            client.session.set_keyspace(KS)
            schema_created = True
        tables = self._tables(client)
        t0 = time.perf_counter()
        for tb in tables:
            client.session.execute(f"TRUNCATE {KS}.{tb}", timeout=300)
        truncate_ms = round((time.perf_counter() - t0) * 1000, 1)
        per_node: dict[str, Any] = {}
        errors: list[str] = []

        def _import(t: Target):
            t1 = time.perf_counter()
            per_table = {}
            for tb in tables:
                t2 = time.perf_counter()
                rc, _, _ = dockerctl.exec_in(t.container, ["test", "-d", f"/backups/{tag}/{t.name}/{KS}/{tb}"], timeout=60)
                if rc != 0:
                    per_table[tb] = None          # table not in the export (created after the backup)
                    continue
                # --copy-data: without it nodetool import MOVES the SSTables out of the export (the artifact is consumed)
                rc, out, err = dockerctl.exec_in(t.container, ["nodetool", "import", "--copy-data", KS, tb, f"/backups/{tag}/{t.name}/{KS}/{tb}"], user="0", timeout=3600)
                per_table[tb] = round((time.perf_counter() - t2) * 1000, 1)
                if rc != 0:
                    errors.append(f"{t.name}/{tb}: {(err or out)[-200:]}")
            per_node[t.name] = {"ms": round((time.perf_counter() - t1) * 1000, 1), "per_table_ms": per_table}

        ths = [threading.Thread(target=_import, args=(t,), daemon=True) for t in nodes]
        for th in ths:
            th.start()
        for th in ths:
            th.join()
        if errors:
            raise EngineError("nodetool import: " + "; ".join(errors)[:600])
        return {"method": "TRUNCATE tables + nodetool import --copy-data of each node's own snapshot export (parallel per node)", "online": True,
                "schema_created": schema_created, "truncate_ms": truncate_ms, "tables": len(tables), "per_node": per_node, "nodetool_invocations": len(tables) * len(nodes),
                "note": "sstableloader (client-side streaming) fails on the vector<float, 8> column in 5.0.x: 'Could not parse type name'"}

    def backup_incremental(self, client: CqlClient, tag: str, base: dict[str, Any], *, log=print) -> dict[str, Any]:
        t = self._node_containers()[0]
        rc, out, err = dockerctl.exec_in(t.container, ["nodetool", "enablebackup"], timeout=120)
        if rc != 0:
            raise EngineError(err[-200:])
        try:
            client.bulk_put("repl_probe", [{"id": 100 + i, "ts_ms": int(time.time() * 1000)} for i in range(1, 11)])
            rc, out, err = dockerctl.exec_in(t.container, ["nodetool", "flush", KS, "repl_probe"], timeout=300)
            rc, out, err = dockerctl.exec_in(t.container, ["sh", "-c", f"find /var/lib/cassandra/data/{KS} -path '*/backups/*' -type f | wc -l; du -cb $(find /var/lib/cassandra/data/{KS} -path '*/backups/*' -type f) 2>/dev/null | tail -1 | cut -f1"], user="0", timeout=300)
            lines = out.split()
        finally:
            dockerctl.exec_in(t.container, ["nodetool", "disablebackup"], timeout=120)
        files = int(lines[0]) if lines else 0
        return {"method": "nodetool enablebackup (incremental_backups) + flush: new SSTables hard-linked into <table>/backups/", "files": files,
                "artifact_bytes_delta": int(lines[1]) if len(lines) > 1 and lines[1].isdigit() else None, "node": t.name}

    # --- logging -----------------------------------------------------------------------------------------
    LOGGING_STRATEGY = {
        "server_log": "logback text: system.log (INFO), debug.log (DEBUG, rolling 20 MB) and gc.log under /opt/cassandra/logs, mirrored to stdout",
        "slow_query": "slow_query_log_timeout (500 ms, cassandra.yaml — not mutable at run time in 5.0.x): MonitoringTask aggregates slow reads every 5 s into debug.log ('<query>, time N msec - slow timeout 500 msec')",
        "audit": "nodetool enableauditlog (BinAuditLogger under logs/audit, read with auditlogviewer; FileAuditLogger via logback) + full query log (enablefullquerylog / fqltool)",
        "runtime": "nodetool setlogginglevel <logger> <level> per node (logback), getlogginglevels; audit/FQL/incremental backups toggled with nodetool too",
    }

    def server_log_facts(self, client: CqlClient) -> dict[str, Any]:
        t = self.cfg.primary
        if not t.container:
            return {"format": "text", "where": "$CASSANDRA_LOG_DIR"}
        rc, out, _ = dockerctl.exec_in(t.container, ["sh", "-c", "ls -la /opt/cassandra/logs | tail -n +2; tail -n 2 /opt/cassandra/logs/system.log"], timeout=60)
        lines = out.splitlines()
        return {"format": "text", "where": "/opt/cassandra/logs/{system,debug,gc}.log + stdout", "files": [ln[:120] for ln in lines if ".log" in ln][:6], "sample": [ln[:200] for ln in lines[-2:]]}

    def slow_query_setup(self, client: CqlClient, threshold_ms: int) -> Any:
        try:
            client.session.execute(f"UPDATE system_views.settings SET value = '{threshold_ms}ms' WHERE name = 'slow_query_log_timeout'", timeout=30)
            eff = threshold_ms
            how = f"UPDATE system_views.settings slow_query_log_timeout={threshold_ms}ms"
        except Exception as e:  # noqa: BLE001
            r = client.session.execute("SELECT value FROM system_views.settings WHERE name = 'slow_query_log_timeout'", timeout=30).one()
            eff = int(str(r[0]).rstrip("ms")) if r and str(r[0]).endswith("ms") else 500
            how = f"slow_query_log_timeout={eff}ms from cassandra.yaml (settings vtable is read-only: {short_err(e)[:60]})"
        t = self.cfg.primary
        if t.container:
            dockerctl.exec_in(t.container, ["sh", "-c", "grep -c 'slow timeout' /opt/cassandra/logs/debug.log > /tmp/nslab_slow_before || echo 0 > /tmp/nslab_slow_before"], user="0", timeout=60)
        return (how + " -> debug.log via MonitoringTask (5 s report interval)", eff)

    def slow_query_fast_op(self, client: CqlClient) -> str:
        client.ex("SELECT id FROM customers WHERE id = ?", (1,))
        return "SELECT id FROM customers WHERE id = 1"

    def slow_query_slow_op(self, client: CqlClient) -> str:
        client.ex_simple("SELECT COUNT(*) FROM orders", timeout=600).one()
        return "SELECT COUNT(*) FROM orders (200k-partition cluster-wide read)"

    def slow_query_entries(self, client: CqlClient, threshold_ms: int) -> list[dict[str, Any]]:
        t = self.cfg.primary
        if not t.container:
            raise Unsupported("needs node access to debug.log")
        rc, out, _ = dockerctl.exec_in(t.container, ["sh", "-c", "n=$(cat /tmp/nslab_slow_before 2>/dev/null || echo 0); grep -h 'slow timeout' /opt/cassandra/logs/debug.log | tail -n +$((n+1)) | tail -n 20"], user="0", timeout=60)
        entries = []
        for ln in out.splitlines():
            m = re.search(r"<(.*)>, time (\d+) msec - slow timeout (\d+) msec", ln)
            if m:
                entries.append({"ms": int(m.group(2)), "text": m.group(1)[:200], "threshold_ms": int(m.group(3))})
        return entries

    def audit_log_setup(self, client: CqlClient) -> str:
        t = self.cfg.primary
        if not t.container:
            raise Unsupported("nodetool enableauditlog needs node access")
        dockerctl.exec_in(t.container, ["sh", "-c", "rm -rf /opt/cassandra/logs/audit"], user="0", timeout=60)
        rc, out, err = dockerctl.exec_in(t.container, ["nodetool", "enableauditlog", "--included-keyspaces", KS], timeout=120)
        if rc != 0:
            raise EngineError(f"enableauditlog: {(err or out)[-200:]}")
        return f"nodetool enableauditlog --included-keyspaces {KS} (BinAuditLogger -> /opt/cassandra/logs/audit, Chronicle queue)"

    def audit_log_exercise(self, client: CqlClient) -> list[str]:
        stmts = ["SELECT id FROM customers WHERE id = 42", "INSERT INTO scratch (k, v) VALUES ('audit-probe', 'x')", "DELETE FROM scratch WHERE k = 'audit-probe'"]
        for q in stmts:
            client.ex_simple(q, timeout=30)
        return stmts

    def audit_log_entries(self, client: CqlClient) -> list[str]:
        t = self.cfg.primary
        dockerctl.exec_in(t.container, ["nodetool", "disableauditlog"], timeout=120)
        rc, out, err = dockerctl.exec_in(t.container, ["sh", "-c", "/opt/cassandra/tools/bin/auditlogviewer /opt/cassandra/logs/audit 2>/dev/null | grep '^LogMessage:' | tail -n 60"], user="0", timeout=180)
        lines = [ln.replace("LogMessage: ", "", 1).strip() for ln in out.splitlines() if ln.startswith("LogMessage:")]
        keep = [ln for ln in lines if "audit-probe" in ln or "customers WHERE id = 42" in ln]
        return keep or lines

    def audit_log_teardown(self, client: CqlClient) -> None:
        t = self.cfg.primary
        dockerctl.exec_in(t.container, ["nodetool", "disableauditlog"], timeout=120)

    def log_level_runtime(self, client: CqlClient) -> str:
        t = self.cfg.primary
        if not t.container:
            raise Unsupported("nodetool setlogginglevel needs node access")
        logger = "org.apache.cassandra.db.Keyspace"
        rc, out, err = dockerctl.exec_in(t.container, ["nodetool", "setlogginglevel", logger, "TRACE"], timeout=120)
        if rc != 0:
            raise EngineError((err or out)[-200:])
        rc, out, _ = dockerctl.exec_in(t.container, ["nodetool", "getlogginglevels"], timeout=120)
        seen = next((ln.split()[-1] for ln in out.splitlines() if logger in ln), None)
        dockerctl.exec_in(t.container, ["nodetool", "setlogginglevel", logger, "DEBUG"], timeout=120)
        if seen != "TRACE":
            raise EngineError(f"level not applied (getlogginglevels shows {seen})")
        return f"nodetool setlogginglevel {logger} TRACE -> verified -> DEBUG (per node, no restart)"

    def promote(self, target: Target) -> str:
        return "leaderless: no promotion needed (remaining replicas serve LOCAL_QUORUM with RF=3)"
