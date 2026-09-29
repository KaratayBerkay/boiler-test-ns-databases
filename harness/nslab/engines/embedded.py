"""Embedded key-value adapters: LMDB (memory-mapped B+tree) and RocksDB (LSM) — in-process, no containers.

These have no query engine at all: they are ordered key-value maps. The lab models the dataset the way an
application on an embedded KV store would — a primary keyspace plus hand-maintained ordered index keyspaces:
  <coll>/<id>                     msgpack/json-encoded document (LMDB: named sub-DB per collection)
  idx/orders_by_customer/<cid>/<id>              -> b""    (range_by_secondary; ordered key scan)
  idx/orders_by_time/<ts:020d>/<id>              -> b""    (pagination via ordered range + cursor)
  idx/products_by_cat/<cid>/<price:012d>/<id>    -> b""    (top_n via reverse range)
  idx/events_by_type/<type>/<ts:020d>/<id>       -> b""    (range_by_time; the optimisation phase drops/rebuilds it)
Aggregations, text, geo and vector have no index: they scan the primary keyspace (the true cost on a KV store).
Concurrency: one writer process owns the file, so the harness uses threads (LMDB is multi-reader/single-writer with
MVCC; RocksDB is thread-safe). Both run entirely under `data/<engine>/`.
"""
from __future__ import annotations

import datetime as dt
import json
import math
import struct
import time
from pathlib import Path
from typing import Any, Iterator

from ..config import Target
from ..datagen import read_docs
from ..ops import Unsupported
from ..util import DATA
from .base import Client, Engine, EngineError, batched, iso

COLLS = ["customers", "categories", "products", "orders", "order_items", "events", "inventory", "repl_probe", "scratch"]

_HANDLES: dict[str, Any] = {}          # process-global open DB handles keyed by file path (LMDB/RocksDB forbid a second open in the same process)


def enc(doc: dict) -> bytes:
    return json.dumps(doc, separators=(",", ":"), default=iso).encode()


def dec(b: bytes | None) -> dict | None:
    return json.loads(b) if b else None


def ts_key(v: Any) -> str:
    if isinstance(v, str):
        v = dt.datetime.strptime(v, "%Y-%m-%dT%H:%M:%SZ")
    return f"{int(v.replace(tzinfo=dt.timezone.utc).timestamp()):020d}"


class KVClient(Client):
    """Shared logic for LMDB and RocksDB; subclasses provide the storage primitives get/set/dele/scan."""

    def close(self) -> None:
        pass          # the DB handle is shared across all clients/threads of the engine; the process owns it

    # --- storage primitives (subclass) ---------------------------------------------------------
    def _get(self, k: bytes) -> bytes | None: raise NotImplementedError
    def _set_many(self, items: list[tuple[bytes, bytes]]) -> None: raise NotImplementedError
    def _del_many(self, keys: list[bytes]) -> None: raise NotImplementedError
    def _scan_prefix(self, prefix: bytes, *, values: bool = True, reverse: bool = False, limit: int | None = None) -> Iterator[tuple[bytes, bytes]]: raise NotImplementedError
    def _range(self, lo: bytes, hi: bytes, *, limit: int | None = None) -> Iterator[tuple[bytes, bytes]]: raise NotImplementedError
    def _count_prefix(self, prefix: bytes) -> int: raise NotImplementedError

    # --- key helpers ----------------------------------------------------------------------------
    @staticmethod
    def dk(coll: str, key: Any) -> bytes:
        return f"d/{coll}/{key}".encode()

    def ping(self):
        return True

    def server_version(self) -> str:
        return self.engine.version_string()

    def role(self) -> str:
        return "embedded"

    def count(self, coll: str) -> int:
        return self._count_prefix(f"d/{coll}/".encode())

    def get(self, coll: str, key: Any) -> dict | None:
        return dec(self._get(self.dk(coll, key)))

    def put(self, coll: str, doc: dict) -> None:
        self.bulk_put(coll, [doc])

    def delete(self, coll: str, key: Any) -> None:
        self._del_many([self.dk(coll, key)])

    def _index_items(self, coll: str, d: dict) -> list[tuple[bytes, bytes]]:
        out: list[tuple[bytes, bytes]] = []
        if coll == "orders":
            out.append((f"i/o_cust/{d['customer_id']}/{d['id']:012d}".encode(), enc({"id": d["id"], "status": d["status"], "amt": d["total_amount"]})))
            out.append((f"i/o_time/{ts_key(d['ordered_at'])}/{d['id']:012d}".encode(), b""))
            out.append((f"i/o_status/{d['status']}/{d['id']:012d}".encode(), b""))
        elif coll == "events":
            if self.engine.events_index:
                out.append((f"i/e_type/{d['event_type']}/{ts_key(d['occurred_at'])}/{d['id']:015d}".encode(),
                            enc({"id": d["id"], "cid": d["customer_id"], "ts": d["occurred_at"] if isinstance(d["occurred_at"], str) else iso(d["occurred_at"])})))
        elif coll == "products":
            out.append((f"i/p_cat/{d['category_id']}/{int(float(d['price'])*100):012d}/{d['id']:08d}".encode(), enc({"id": d["id"], "price": d["price"], "active": d["active"]})))
        elif coll == "order_items":
            out.append((f"i/it_order/{d['order_id']}/{d['id']:012d}".encode(), b""))
        return out

    def bulk_put(self, coll: str, docs: list[dict]) -> int:
        items = []
        for d in docs:
            if coll == "inventory":
                key = f"{d['product_id']}:{d['warehouse_id']}"
            elif coll == "scratch":
                key = d["id"]
            else:
                key = d["id"]
            items.append((self.dk(coll, key), enc(d)))
            items.extend(self._index_items(coll, d))
        self._set_many(items)
        return len(docs)

    def delete_many(self, coll: str, keys: list[Any]) -> None:
        self._del_many([self.dk(coll, k) for k in keys])

    def drop_collection(self, coll: str) -> None:
        self._del_prefix(f"d/{coll}/".encode())
        pref = {"orders": [b"i/o_cust/", b"i/o_time/", b"i/o_status/"], "events": [b"i/e_type/"], "products": [b"i/p_cat/"], "order_items": [b"i/it_order/"]}.get(coll, [])
        for p in pref:
            self._del_prefix(p)

    def _del_prefix(self, prefix: bytes) -> None:
        while True:
            keys = [k for k, _ in self._scan_prefix(prefix, values=False, limit=50000)]
            if not keys:
                break
            self._del_many(keys)

    # --- operations ----------------------------------------------------------------------------------
    def op_get_by_key(self, p):
        d = self.get("customers", p["id"])
        return [(d["id"], d["name"], d["email"], d["country_code"])] if d else []

    def op_multi_get(self, p):
        out = []
        for i in p["ids"]:
            d = self.get("customers", i)
            if d:
                out.append((d["id"], d["tier"]))
        return sorted(out)

    def op_insert_one(self, p):
        self.put("events", p)
        return None

    def op_update_field(self, p):
        d = self.get("customers", p["id"])
        if d:
            d["last_login"] = p["value"]
            self._set_many([(self.dk("customers", p["id"]), enc(d))])
        return None

    def op_upsert(self, p):
        self.put("inventory", {"product_id": p["product_id"], "warehouse_id": p["warehouse_id"], "qty": p["qty"], "updated_at": iso(dt.datetime.utcnow()), "version": 1})
        return None

    def op_delete_one(self, p):
        self.delete("events", p["id"])
        return None

    def op_atomic_increment(self, p):
        # single-writer store: the harness serialises writes, so read-modify-write is effectively atomic
        k = f"{p['product_id']}:{p['warehouse_id']}"
        d = self.get("inventory", k)
        d["qty"] += 1
        self._set_many([(self.dk("inventory", k), enc(d))])
        return [(d["qty"],)]

    def op_conditional_update(self, p):
        k = f"{p['product_id']}:{p['warehouse_id']}"
        d = self.get("inventory", k)
        if not d:
            return [(0, -1)]
        d["qty"] = 250
        d["version"] += 1
        self._set_many([(self.dk("inventory", k), enc(d))])
        return [(1, d["version"])]

    def op_batch_insert_1000(self, p):
        self.bulk_put("events", p["docs"])
        return None

    def op_range_by_secondary(self, p):
        out = []
        for _, v in self._scan_prefix(f"i/o_cust/{p['customer_id']}/".encode()):
            d = json.loads(v)
            out.append((d["id"], d["status"], d["amt"]))
        return out

    def op_range_by_time(self, p):
        if next(iter(self._scan_prefix(b"i/e_type/", values=False, limit=1)), None) is None:
            return self._range_by_time_scan(p)          # index absent (dropped by opt01): full keyspace scan
        lo = f"i/e_type/{p['event_type']}/{ts_key(p['start'])}".encode()
        hi = f"i/e_type/{p['event_type']}/{ts_key(p['end'])}".encode()
        out = []
        for _, v in self._range(lo, hi, limit=p["limit"]):
            d = json.loads(v)
            out.append((d["id"], d["cid"], d["ts"]))
        return out

    def _range_by_time_scan(self, p):
        lo, hi = iso(p["start"]), iso(p["end"])
        out = []
        for _, v in self._scan_prefix(b"d/events/"):
            d = json.loads(v)
            if d["event_type"] == p["event_type"] and lo <= d["occurred_at"] < hi:
                out.append((d["id"], d["customer_id"], d["occurred_at"]))
        out.sort(key=lambda t: (t[2], t[0]))
        return out[:p["limit"]]

    def op_filter_nested(self, p):
        n = 0
        for _, v in self._scan_prefix(b"d/customers/"):
            d = json.loads(v)
            if d.get("tier") == "pro" and (d.get("attrs") or {}).get("lang") == "tr":
                n += 1
        return [(n,)]

    def op_array_contains(self, p):
        n = 0
        for _, v in self._scan_prefix(b"d/products/"):
            d = json.loads(v)
            if d.get("active") and "eco" in (d.get("attrs") or {}).get("tags", []):
                n += 1
        return [(n,)]

    def op_top_n_sorted(self, p):
        out = []
        for _, v in self._scan_prefix(f"i/p_cat/{p['category_id']}/".encode(), reverse=True):
            d = json.loads(v)
            if d["active"]:
                out.append((d["id"], d["price"]))
            if len(out) >= p["n"]:
                break
        return out

    def op_pagination_offset(self, p):
        out = []
        for i, (k, _) in enumerate(self._scan_prefix(b"i/o_time/", values=False)):
            if i < p["offset"]:
                continue
            parts = k.decode().split("/")
            out.append((int(parts[3]), None))
            if len(out) >= p["page"]:
                break
        return [(oid, self.get("orders", oid)["ordered_at"]) for oid, _ in out]

    def op_pagination_keyset(self, p):
        cur = getattr(self, "_cursor", None)
        if cur is None:
            for i, (k, _) in enumerate(self._scan_prefix(b"i/o_time/", values=False)):
                if i == p["offset"] - 1:
                    cur = self._cursor = k
                    break
        out = []
        for k, _ in self._range(cur + b"\x00", b"i/o_time/\xff", limit=p["page"]):
            parts = k.decode().split("/")
            oid = int(parts[3])
            out.append((oid, self.get("orders", oid)["ordered_at"]))
        return out

    def op_count_filtered(self, p):
        return [(self._count_prefix(b"i/o_status/pending/"),)]

    def op_aggregate_group(self, p):
        agg: dict[str, list] = {}
        for _, v in self._scan_prefix(b"d/orders/"):
            d = json.loads(v)
            a = agg.setdefault(d["shipping_country"], [0, 0.0])
            a[0] += 1
            a[1] += d["total_amount"]
        return [(c, n, round(s, 2)) for c, (n, s) in sorted(agg.items())]

    def op_aggregate_time_bucket(self, p):
        agg: dict[str, list] = {}
        for _, v in self._scan_prefix(b"d/events/"):
            d = json.loads(v)
            if d["event_type"] != "checkout":
                continue
            k = d["occurred_at"][:7]
            a = agg.setdefault(k, [0, 0.0])
            a[0] += 1
            a[1] += d["value_num"]
        return [(m, n, round(s, 4)) for m, (n, s) in sorted(agg.items())]

    def op_join_lookup(self, p):
        name = (self.get("customers", p["customer_id"]) or {}).get("name")
        out = []
        for _, v in self._scan_prefix(f"i/o_cust/{p['customer_id']}/".encode()):
            d = json.loads(v)
            out.append((d["id"], name, d["amt"]))
        return out

    def op_graph_traversal(self, p):
        prod = self.get("products", p["product_id"])
        cat = prod["category_id"] if prod else None
        path = []
        while cat is not None and len(path) < 10:
            c = self.get("categories", cat)
            if not c:
                break
            path.append((c["depth"], c["id"]))
            cat = c.get("parent_id")
        return sorted(path)

    def op_text_search(self, p):
        words = [w.lower() for w in p["words"]]
        out = []
        for _, v in self._scan_prefix(b"d/products/"):
            d = json.loads(v)
            if d.get("description") and all(w in d["description"] for w in words):
                out.append(d["id"])
        return [(i,) for i in sorted(out)[:p["limit"]]]

    def op_geo_near(self, p):
        lat0, lon0 = math.radians(p["lat"]), math.radians(p["lon"])
        hits = []
        for _, v in self._scan_prefix(b"d/customers/"):
            d = json.loads(v)
            loc = d.get("location")
            if not loc:
                continue
            la, lo = math.radians(loc["lat"]), math.radians(loc["lon"])
            h = math.sin((la - lat0) / 2) ** 2 + math.cos(lat0) * math.cos(la) * math.sin((lo - lon0) / 2) ** 2
            dist = 2 * 6371.0 * math.asin(math.sqrt(h))
            if dist <= p["km"]:
                hits.append((dist, d["id"]))
        hits.sort()
        return [(i,) for _, i in hits[:p["limit"]]]

    def op_vector_knn(self, p):
        q = p["vector"]
        best = []
        for _, v in self._scan_prefix(b"d/products/"):
            d = json.loads(v)
            e = d.get("embedding")
            if not e:
                continue
            sim = sum(a * b for a, b in zip(q, e))          # both are unit vectors -> cosine
            best.append((sim, d["id"]))
        best.sort(reverse=True)
        return [(i,) for _, i in best[:p["k"]]]

    def op_transaction_transfer(self, p):
        a = self.get("inventory", f"{p['product_id']}:{p['from']}")
        b = self.get("inventory", f"{p['product_id']}:{p['to']}")
        a["qty"] -= p["amount"]
        b["qty"] += p["amount"]
        self._set_many([(self.dk("inventory", f"{p['product_id']}:{p['from']}"), enc(a)),
                        (self.dk("inventory", f"{p['product_id']}:{p['to']}"), enc(b))])   # one write batch = atomic
        return None

    def op_ttl_set(self, p):
        self._set_many([(self.dk("scratch", p["key"]), enc({"v": p["value"], "expires_at": iso(dt.datetime.utcnow() + dt.timedelta(seconds=p["seconds"]))}))])
        return None

    def op_scan_full(self, p):
        return [(self._count_prefix(b"d/events/"),)]

    # --- optimisation hooks -----------------------------------------------------------------------------
    def opt_fetch_full(self, coll, key):
        d = self.get(coll, key)
        return [(d["id"], len(d))] if d else []

    def opt_fetch_projected(self, coll, key, fields):
        d = self.get(coll, key)
        return [tuple(d.get(f) for f in fields)] if d else []

    def opt_order_embedded(self, order_id):
        d = self.get("orders", order_id)
        return [(d["id"], len(d.get("items", [])))]

    def opt_order_referenced(self, order_id):
        d = self.get("orders", order_id)
        n = self._count_prefix(f"i/it_order/{order_id}/".encode())
        return [(d["id"], n)]

    def opt_write_durable(self, level, doc):
        self._durability = level
        self.put("events", doc)
        return None

    def opt_read_level(self, level, key):
        return self.op_get_by_key({"id": key})

    # --- capability probes ------------------------------------------------------------------------
    STATIC_PROBES = {
        "secondary_index": ("unsupported", "no query engine; application-maintained ordered index keyspaces (idx/...)"),
        "compound_index": ("unsupported", "composite ordered keys built by hand"),
        "unique_constraint": ("unsupported", "keys are unique; no constraint on values"),
        "ttl_expiry": ("unsupported", "no native TTL (store an expiry field and sweep; RocksDB has a TTL column-family option)"),
        "text_index": ("unsupported", "full keyspace scan (no inverted index)"),
        "geo_index": ("unsupported", "full keyspace scan + haversine"),
        "vector_index": ("unsupported", "full keyspace scan (brute-force cosine)"),
        "aggregation": ("unsupported", "client-side scan"),
        "server_side_join": ("unsupported", "client-side"),
        "graph_traversal": ("unsupported", "client-side pointer chasing"),
        "schema_validation": ("unsupported", "opaque byte values"),
        "change_streams": ("unsupported", "no change feed (RocksDB has a WAL/EventListener at the C++ level, not exposed here)"),
        "server_side_scripting": ("unsupported", "in-process library: 'server-side' is your own code"),
        "pubsub": ("unsupported", "no messaging"),
        "explain_plan": ("unsupported", "no planner; access is whatever key/range you request"),
        "many_fields": ("supported", "value is an opaque blob; field count is up to your encoding"),
        "read_from_replica": ("unsupported", "single local file; replication is external (Litestream-style log shipping)"),
        "snapshot_backup": ("supported", "consistent on-disk copy while open (LMDB: env.copy(compact) / mdb_copy -c; RocksDB: Checkpoint hard links) — exercised in the backup phase"),
        "slow_query_log": ("unsupported", "no server: time the calls in the application (a decorator around txn/get/scan)"),
        "audit_log": ("unsupported", "no server: audit in the application"),
        "structured_logs": ("unsupported", "LMDB has no log; RocksDB writes its info LOG (text) into the DB dir"),
        "runtime_log_level": ("unsupported", "RocksDB info_log_level / stats period are open-time options; LMDB has none"),
        "incremental_backup": ("unsupported", "LMDB: full copy only; RocksDB BackupEngine does incremental (not exposed by rocksdict, Checkpoint is full via hard links)"),
        "pitr": ("unsupported", "no change log to replay (RocksDB WAL is not a PITR log); Litestream-style shipping is external"),
    }

    def probe_partial_update(self):
        d = self.get("customers", 1)
        d["probe"] = 1
        self._set_many([(self.dk("customers", 1), enc(d))])
        return "read-modify-write the whole value (no in-place field update; the value is an opaque blob)"

    def probe_atomic_increment(self):
        return f"read-modify-write under the single-writer lock -> {self.op_atomic_increment({'product_id':1,'warehouse_id':1})} (atomic because one writer at a time)"

    def probe_optimistic_concurrency(self):
        return f"application-level CAS: read value, compare, write in one write txn/batch -> {self.op_conditional_update({'product_id':1,'warehouse_id':1})}"

    def probe_multi_doc_transaction(self):
        self.op_transaction_transfer({"product_id": 1, "from": 1, "to": 2, "amount": 0})
        return "atomic multi-key write batch (LMDB: one write transaction, fully ACID; RocksDB: WriteBatch, atomic + durable)"

    def probe_tunable_consistency(self):
        return "single local writer: reads are always consistent within a transaction snapshot (LMDB MVCC; RocksDB snapshot)"

    def probe_array_operators(self):
        raise Unsupported("values are opaque; array handling is in your encoding/decoding")

    def probe_bulk_import(self):
        return "batched write transaction / WriteBatch (what the load phase uses)"

    def probe_large_document(self):
        blob = "x" * (15 * 1024 * 1024)
        self._set_many([(self.dk("scratch", "big"), enc({"blob": blob}))])
        n = len(self.get("scratch", "big")["blob"])
        self._del_many([self.dk("scratch", "big")])
        return f"{n/1048576:.0f} MB value ok (LMDB single value limit is bounded by map size; RocksDB by memory)"


class LmdbClient(KVClient):
    def _txn(self, write: bool):
        return self.raw.begin(write=write)

    def _get(self, k):
        with self._txn(False) as t:
            return t.get(k)

    def _set_many(self, items):
        with self._txn(True) as t:
            for k, v in items:
                t.put(k, v)

    def _del_many(self, keys):
        with self._txn(True) as t:
            for k in keys:
                t.delete(k)

    def _scan_prefix(self, prefix, *, values=True, reverse=False, limit=None):
        with self._txn(False) as t:
            cur = t.cursor()
            n = 0
            if reverse:
                # position past the prefix and step backwards
                if not cur.set_range(prefix + b"\xff"):
                    cur.last()
                else:
                    cur.prev()
                while cur.key().startswith(prefix) if cur.key() else False:
                    yield (bytes(cur.key()), bytes(cur.value()) if values else b"")
                    n += 1
                    if limit and n >= limit:
                        return
                    if not cur.prev():
                        break
            else:
                if cur.set_range(prefix):
                    for k, v in cur:
                        if not k.startswith(prefix):
                            break
                        yield (bytes(k), bytes(v) if values else b"")
                        n += 1
                        if limit and n >= limit:
                            return

    def _range(self, lo, hi, *, limit=None):
        with self._txn(False) as t:
            cur = t.cursor()
            n = 0
            if cur.set_range(lo):
                for k, v in cur:
                    if k >= hi:
                        break
                    yield (bytes(k), bytes(v))
                    n += 1
                    if limit and n >= limit:
                        return

    def _count_prefix(self, prefix):
        n = 0
        for _ in self._scan_prefix(prefix, values=False):
            n += 1
        return n


class RocksClient(KVClient):
    def _get(self, k):
        return self.raw.get(k)

    def _set_many(self, items):
        from rocksdict import WriteBatch
        wb = WriteBatch()
        for k, v in items:
            wb.put(k, v)
        self.raw.write(wb)

    def _del_many(self, keys):
        from rocksdict import WriteBatch
        wb = WriteBatch()
        for k in keys:
            wb.delete(k)
        self.raw.write(wb)

    def _scan_prefix(self, prefix, *, values=True, reverse=False, limit=None):
        it = self.raw.iter()
        n = 0
        if reverse:
            it.seek_for_prev(prefix + b"\xff")
            while it.valid():
                k = it.key()
                if not k.startswith(prefix):
                    break
                yield (k, it.value() if values else b"")
                n += 1
                if limit and n >= limit:
                    return
                it.prev()
        else:
            it.seek(prefix)
            while it.valid():
                k = it.key()
                if not k.startswith(prefix):
                    break
                yield (k, it.value() if values else b"")
                n += 1
                if limit and n >= limit:
                    return
                it.next()

    def _range(self, lo, hi, *, limit=None):
        it = self.raw.iter()
        it.seek(lo)
        n = 0
        while it.valid():
            k = it.key()
            if k >= hi:
                break
            yield (k, it.value())
            n += 1
            if limit and n >= limit:
                return
            it.next()

    def _count_prefix(self, prefix):
        n = 0
        for _ in self._scan_prefix(prefix, values=False):
            n += 1
        return n


class _EmbeddedEngine(Engine):
    embedded = True
    supports_processes = False
    load_batch = 5000
    load_parse_dates = False        # documents are stored as JSON; keep timestamps as ISO strings
    events_index = True

    def apply_features(self, features: dict) -> None:
        self.events_index = bool(features.get("events_index", True))
        self.path = DATA / self.driver
        self._handle = None          # single shared DB handle (in-process file; threads share it)

    def _open(self):
        raise NotImplementedError

    def connect(self, target: Target, *, timeout: float = 10.0):
        key = str(self.path)
        h = _HANDLES.get(key)
        if h is None:
            h = _HANDLES[key] = self._open()
        return self._client_cls(self, target, h)

    def collections(self) -> list[str]:
        from ..schema import LOAD_ORDER
        return list(LOAD_ORDER)

    def create_index(self, client, name, coll, fields):
        if coll == "events":
            n = 0
            items = []
            for _, v in client._scan_prefix(b"d/events/"):
                d = json.loads(v)
                items.append((f"i/e_type/{d['event_type']}/{ts_key(d['occurred_at'])}/{d['id']:015d}".encode(),
                              enc({"id": d["id"], "cid": d["customer_id"], "ts": d["occurred_at"]})))
                if len(items) >= 20000:
                    client._set_many(items)
                    n += len(items)
                    items = []
            if items:
                client._set_many(items)
                n += len(items)
            return f"built i/e_type/* ordered index over {n} events"
        raise Unsupported("only the events-by-type index is modelled")

    def drop_index(self, client, name, coll):
        client._del_prefix(b"i/e_type/")

    def index_variants(self):
        return [("no_index_scan", None), ("ordered_index_(type,ts,id)", ["event_type", "occurred_at"])]

    def durability_levels(self) -> list[str]:
        return []

    def read_levels(self):
        return []

    def replication_status(self, client):
        return {}

    # --- backup / restore (shared) -----------------------------------------------------------------------
    def _backup_dir(self, tag: str) -> Path:
        d = DATA / f"{self.driver}-backups" / tag
        d.mkdir(parents=True, exist_ok=True)
        return d

    @staticmethod
    def _dir_bytes(p: Path) -> int:
        if p.is_file():
            return p.stat().st_size
        return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())

    def _reopen(self):
        key = str(self.path)
        h = _HANDLES.pop(key, None)
        if h is not None:
            try:
                h.close()
            except Exception:  # noqa: BLE001
                pass
        _HANDLES[key] = self._open()
        return _HANDLES[key]

    def artifact_paths(self, tag: str) -> list[tuple[str, str]]:
        return [("host", str(self._backup_dir(tag)))]

    def wipe(self) -> None:
        import shutil
        key = str(self.path)
        h = _HANDLES.pop(key, None)
        if h is not None:
            try:
                h.close()
            except Exception:  # noqa: BLE001
                pass
        for f in self.path.glob("lab.*"):
            shutil.rmtree(f) if f.is_dir() else f.unlink()

    LOGGING_STRATEGY = {
        "server_log": "no server process: the host application logs (Python logging / structlog) — the harness writes results/logs/nslab-*.jsonl",
        "slow_query": "instrument the call site (timing decorator, OpenTelemetry span per transaction)",
        "audit": "application-level",
        "runtime": "application-level",
    }

    def server_log_facts(self, client):
        return {"format": "none", "where": "in-process library: log in the host application"}

    def slow_query_setup(self, client, threshold_ms: int) -> str:
        raise Unsupported("no server: time the calls in the application (decorator / OpenTelemetry span per transaction)")

    def audit_log_setup(self, client) -> str:
        raise Unsupported("no server: audit in the application")

    def log_level_runtime(self, client) -> str:
        raise Unsupported("no server log level (RocksDB info_log_level is an open-time option; LMDB has no log)")


class LmdbEngine(_EmbeddedEngine):
    driver = "lmdb"
    _client_cls = LmdbClient

    def version_string(self) -> str:
        import lmdb
        return f"LMDB {'.'.join(str(x) for x in lmdb.version())} (python-lmdb {lmdb.__version__})"

    def _open(self):
        import lmdb
        self.path.mkdir(parents=True, exist_ok=True)
        return lmdb.open(str(self.path / "lab.mdb"), map_size=8 * 1024**3, subdir=False, writemap=True, map_async=True,
                         max_dbs=1, readahead=False, max_readers=256, lock=True)

    def reset(self, client: LmdbClient) -> None:
        with client.raw.begin(write=True) as t:
            t.drop(client.raw.open_db(), delete=False)

    BACKUP_STRATEGY = {
        "tool": "env.copy(path, compact=True) (== mdb_copy -c): a consistent copy taken inside one read transaction while the app keeps writing",
        "consistency": "transaction snapshot (MVCC read txn pins the pages)",
        "restore": "offline for the process: close the environment, replace the data file, reopen",
        "incremental": "none (single file; copy is always full — compact drops free pages)",
        "pitr": "none",
        "schedule": "the application (or mdb_copy from cron; the file can be copied while open)",
        "retention": "external",
    }

    def data_bytes(self, client) -> int | None:
        # the file is a sparse 8 GB map: used pages x page size is the live size
        try:
            return int(client.raw.info()["last_pgno"]) * int(client.raw.stat()["psize"])
        except Exception:  # noqa: BLE001
            f = self.path / "lab.mdb"
            return f.stat().st_size if f.exists() else None

    def backup(self, client, tag: str, *, log=print):
        dst = self._backup_dir(tag) / "lab.mdb"
        if dst.exists():
            dst.unlink()
        client.raw.copy(str(dst), compact=True)
        return {"method": "lmdb env.copy(compact=True) inside a read txn", "online": True, "artifact": str(dst), "artifact_bytes": dst.stat().st_size,
                "map_size": client.raw.info().get("map_size"), "last_pgno": client.raw.info().get("last_pgno")}

    def restore(self, client, tag: str, info, *, log=print):
        import shutil
        src = self._backup_dir(tag) / "lab.mdb"
        t0 = time.perf_counter()
        key = str(self.path)
        h = _HANDLES.pop(key, None)
        if h is not None:
            h.close()
        shutil.copyfile(src, self.path / "lab.mdb")
        lock = self.path / "lab.mdb-lock"
        if lock.exists():
            lock.unlink()
        _HANDLES[key] = self._open()
        return {"method": "close env; copy backup file over lab.mdb; reopen", "online": False, "downtime_ms": round((time.perf_counter() - t0) * 1000, 1)}


class RocksEngine(_EmbeddedEngine):
    driver = "rocksdb"
    _client_cls = RocksClient

    def version_string(self) -> str:
        import rocksdict
        return f"RocksDB via rocksdict {getattr(rocksdict, '__version__', '?')}"

    def _open(self):
        from rocksdict import Rdict, Options
        self.path.mkdir(parents=True, exist_ok=True)
        opts = Options()
        opts.create_if_missing(True)
        opts.increase_parallelism(4)
        opts.set_max_background_jobs(4)
        opts.set_write_buffer_size(256 * 1024 * 1024)
        # logging strategy for the embedded LSM: rotate the info LOG and dump rocksdb.stats into it every 60 s
        opts.set_max_log_file_size(50 * 1024 * 1024)
        opts.set_keep_log_file_num(3)
        opts.set_stats_dump_period_sec(60)
        return Rdict(str(self.path / "lab.rocks"), options=opts)

    def reset(self, client: RocksClient) -> None:
        client._del_prefix(b"d/")
        client._del_prefix(b"i/")

    BACKUP_STRATEGY = {
        "tool": "Checkpoint.create_checkpoint(dir): hard-links the immutable SSTs + copies MANIFEST/CURRENT/WAL — an openable DB in milliseconds; BackupEngine (C++) adds incremental backups",
        "consistency": "point-in-time (checkpoint of the current version)",
        "restore": "offline for the process: close, replace the DB directory with the checkpoint, reopen",
        "incremental": "BackupEngine only (not exposed by rocksdict); checkpoints share SST files through hard links",
        "pitr": "none (the WAL is for crash recovery, not replay to a timestamp)",
        "schedule": "the application (checkpoint on a timer, then rsync/upload the directory)",
        "retention": "delete old checkpoint dirs (SSTs are freed when the last link goes)",
    }

    def data_bytes(self, client) -> int | None:
        d = self.path / "lab.rocks"
        return self._dir_bytes(d) if d.exists() else None

    def backup(self, client, tag: str, *, log=print):
        from rocksdict import Checkpoint
        import shutil
        dst = self._backup_dir(tag) / "lab.rocks"
        if dst.exists():
            shutil.rmtree(dst)
        client.raw.flush()
        Checkpoint(client.raw).create_checkpoint(str(dst))
        return {"method": "rocksdict Checkpoint.create_checkpoint (hard links) after flush", "online": True, "artifact": str(dst), "artifact_bytes": self._dir_bytes(dst),
                "live_files": len(client.raw.live_files()), "log_facts": self._log_facts()}

    def _log_facts(self) -> dict:
        d = self.path / "lab.rocks"
        logs = sorted(d.glob("LOG*"))
        out = {"info_log_files": [f.name for f in logs][:5], "info_log_bytes": sum(f.stat().st_size for f in logs), "rotation": "max_log_file_size 50 MB, keep_log_file_num 3, stats dump every 60 s"}
        if logs:
            tail = (d / "LOG").read_text(errors="replace").splitlines()[-2:] if (d / "LOG").exists() else []
            out["sample"] = [ln[:200] for ln in tail]
        return out

    def restore(self, client, tag: str, info, *, log=print):
        import shutil
        src = self._backup_dir(tag) / "lab.rocks"
        t0 = time.perf_counter()
        key = str(self.path)
        h = _HANDLES.pop(key, None)
        if h is not None:
            h.close()
        live = self.path / "lab.rocks"
        if live.exists():
            shutil.rmtree(live)
        shutil.copytree(src, live)
        _HANDLES[key] = self._open()
        return {"method": "close; replace lab.rocks with the checkpoint dir; reopen", "online": False, "downtime_ms": round((time.perf_counter() - t0) * 1000, 1)}

    def server_log_facts(self, client):
        from ..util import ROOT
        return {"format": "text (RocksDB info LOG in the DB dir)", "where": str((self.path / "lab.rocks" / "LOG").relative_to(ROOT)), **self._log_facts()}
