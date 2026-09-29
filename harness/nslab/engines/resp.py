"""RESP adapter: Redis, Valkey (standalone or cluster), Dragonfly, KeyDB, Garnet — via redis-py.

Data layout (the portable, module-free way to model documents in a key-value store):
  <coll>:<key>                 hash with the flattened document (attrs.lang, payload.page, ...)
  keys:<coll>                  set of every key of the collection  -> O(1) COUNT
  idx:orders:cust:<cid>        set   (secondary index, built while loading)
  idx:orders:status:<status>   set
  idx:orders:by_time           zset  score = epoch_s * 1e6 + id      (ordered pagination)
  idx:products:cat:<cid>       zset  score = price
  idx:products:tag:<tag>       set
  idx:customers:tier:<tier>    set
  idx:events:type:<type>       zset  score = epoch_s * 1e6 + id      (range_by_time) -- the *optimisation* phase
                               measures SCAN+filter vs this index; the baseline load builds it because a key-value
                               store has no other way to answer that query.
  geo:customers                GEO set (GEOADD)
Inventory keys use a hash tag `inventory:{pid}:w` so both warehouses of a product share a cluster slot (MULTI/EXEC).
With `features.search: true` (Redis 8 / Redis Stack) FT.CREATE indexes provide text and vector search.
"""
from __future__ import annotations

import datetime as _dt
import json
import struct
import time
from pathlib import Path
from typing import Any, Iterable

import redis
from redis.exceptions import ResponseError

from ..config import StackConfig, Target
from ..datagen import read_docs
from ..ops import Unsupported
from ..schema import COLLECTION_BY_NAME
from ..util import short_err
from .. import dockerctl
from .base import Client, Engine, EngineError, batched, epoch_ms, flatten, iso, with_timeout

VEC_DIM = 8


def key_of(coll: str, doc: dict) -> str:
    if coll == "inventory":
        return f"inventory:{{{doc['product_id']}}}:{doc['warehouse_id']}"
    c = COLLECTION_BY_NAME.get(coll)
    if c:
        return f"{coll}:{c.key_of(doc)}"
    return f"{coll}:{doc['id']}"


def inv_key(pid: int, w: int) -> str:
    return f"inventory:{{{pid}}}:{w}"


def to_hash(doc: dict) -> dict[str, Any]:
    out = {}
    for k, v in flatten(doc).items():
        if v is None:
            continue
        if isinstance(v, bool):
            out[k] = "1" if v else "0"
        elif isinstance(v, list):
            if k == "embedding":
                out[k] = struct.pack(f"{len(v)}f", *v)
            elif k == "items":
                out[k] = json.dumps(v)
            else:
                out[k] = ",".join(str(x) for x in v)
        elif hasattr(v, "strftime"):
            out[k] = iso(v)
        else:
            out[k] = v
    return out


def score_of(ts: Any, id_: int) -> float:
    """epoch seconds * 1e6 + id: sortable by (time, id) and < 2^53."""
    if hasattr(ts, "strftime"):
        s = epoch_ms(ts) // 1000
    else:
        s = int(_dt.datetime.fromisoformat(ts).replace(tzinfo=_dt.timezone.utc).timestamp())
    return s * 1_000_000 + id_


_CAS_LUA = """
local v = redis.call('HGET', KEYS[1], 'version')
if v == false then return {0, -1} end
if v ~= ARGV[1] then return {0, tonumber(v)} end
redis.call('HSET', KEYS[1], 'qty', ARGV[2])
local nv = redis.call('HINCRBY', KEYS[1], 'version', 1)
return {1, nv}
"""


class RespClient(Client):
    def __init__(self, engine: "RespEngine", target: Target, raw: Any):
        super().__init__(engine, target, raw)
        self.cluster = isinstance(raw, redis.RedisCluster)
        self.search = bool(engine.features.get("search"))

    def r(self) -> redis.Redis:
        return self.raw

    def pipe(self, transaction: bool = False):
        return self.raw.pipeline(transaction=transaction)

    # --- lifecycle -----------------------------------------------------------------------
    def ping(self):
        return self.raw.ping()

    def server_version(self) -> str:
        try:
            info = self._info("server")
            for k in ("dragonfly_version", "valkey_version", "redis_version", "garnet_version"):
                if k in info:
                    name = k.replace("_version", "")
                    v = f"{name} {info[k]}"
                    if "keydb_version" in info:
                        v = f"keydb {info['keydb_version']} ({v})"
                    return v
            return str(info)[:100]
        except Exception as e:  # noqa: BLE001
            return f"unknown ({e})"

    def _info(self, section: str | None = None) -> dict:
        if self.cluster:
            # info from the node we are talking to (first primary)
            node = self.raw.get_default_node()
            return self.raw.info(section, target_nodes=node) if section else self.raw.info(target_nodes=node)
        return self.raw.info(section) if section else self.raw.info()

    def role(self) -> str:
        try:
            info = self._info("replication")
            r = info.get("role")
            return {"master": "primary", "slave": "replica", "replica": "replica"}.get(r, r or "unknown")
        except Exception:  # noqa: BLE001
            return "unknown"

    # --- primitives ------------------------------------------------------------------------
    def count(self, coll: str) -> int:
        return int(self.raw.scard(f"keys:{coll}"))

    def get(self, coll: str, key: Any) -> dict | None:
        k = key if isinstance(key, str) and key.startswith(coll) else f"{coll}:{key}"
        d = self.raw.hgetall(k)
        return d or None

    def put(self, coll: str, doc: dict) -> None:
        k = key_of(coll, doc)
        p = self.pipe()
        p.hset(k, mapping=to_hash(doc))
        p.sadd(f"keys:{coll}", k)
        p.execute()

    def delete(self, coll: str, key: Any) -> None:
        k = key if isinstance(key, str) and key.startswith(coll) else f"{coll}:{key}"
        p = self.pipe()
        p.delete(k)
        p.srem(f"keys:{coll}", k)
        p.execute()

    def bulk_put(self, coll: str, docs: list[dict]) -> int:
        p = self.pipe()
        keys = []
        for d in docs:
            k = key_of(coll, d)
            keys.append(k)
            p.hset(k, mapping=to_hash(d))
            self._index_doc(p, coll, d, k)
        if self.cluster:
            for k in keys:
                p.sadd(f"keys:{coll}", k)
        else:
            p.sadd(f"keys:{coll}", *keys)
        p.execute()
        return len(docs)

    def _index_doc(self, p, coll: str, d: dict, k: str) -> None:
        if coll == "orders":
            p.sadd(f"idx:orders:cust:{d['customer_id']}", d["id"])
            p.sadd(f"idx:orders:status:{d['status']}", d["id"])
            p.zadd("idx:orders:by_time", {str(d["id"]): score_of(d["ordered_at"], d["id"])})
        elif coll == "order_items":
            p.sadd(f"idx:items:order:{d['order_id']}", d["id"])
        elif coll == "events":
            if self.engine.events_index:
                p.zadd(f"idx:events:type:{d['event_type']}", {str(d["id"]): score_of(d["occurred_at"], d["id"])})
        elif coll == "products":
            p.zadd(f"idx:products:cat:{d['category_id']}", {str(d["id"]): float(d["price"])})
            for t in (d.get("attrs") or {}).get("tags", []):
                p.sadd(f"idx:products:tag:{t}", d["id"])
        elif coll == "customers":
            p.sadd(f"idx:customers:tier:{d['tier']}", d["id"])
            loc = d.get("location")
            if loc:
                p.geoadd("geo:customers", (loc["lon"], loc["lat"], str(d["id"])))

    def delete_many(self, coll: str, keys: list[Any]) -> None:
        p = self.pipe()
        for key in keys:
            k = f"{coll}:{key}"
            p.delete(k)
            p.srem(f"keys:{coll}", k)
        p.execute()

    def drop_collection(self, coll: str) -> None:
        """Delete one collection: its documents (via the keys:<coll> set), that set, and its index keys.
        (Everything shares one keyspace, so this must not FLUSHALL — the engine's reset() does that before a load.)"""
        keyset = f"keys:{coll}"
        cursor = 0
        while True:
            cursor, members = self.raw.sscan(keyset, cursor, count=5000)
            if members:
                p = self.pipe()
                for k in members:
                    p.delete(k)
                p.execute()
            if cursor == 0:
                break
        self.raw.delete(keyset)
        for pattern in (f"idx:{coll}:*", f"geo:{coll}"):
            for k in self.raw.scan_iter(match=pattern, count=1000):
                self.raw.delete(k)

    def flush_all(self) -> None:
        if self.cluster:
            self.raw.flushall(target_nodes=redis.RedisCluster.PRIMARIES)
        else:
            self.raw.flushall()

    # --- helpers ------------------------------------------------------------------------------
    def _hmget_many(self, keys: list[str], fields: list[str]) -> list[list]:
        p = self.pipe()
        for k in keys:
            p.hmget(k, fields)
        return p.execute()

    @staticmethod
    def _num(v):
        if v is None:
            return None
        try:
            return int(v)
        except ValueError:
            return float(v)

    # --- operations ---------------------------------------------------------------------------------
    def op_get_by_key(self, p):
        v = self.raw.hmget(f"customers:{p['id']}", ["id", "name", "email", "country_code"])
        return [(int(v[0]), v[1], v[2], v[3])] if v[0] is not None else []

    def op_multi_get(self, p):
        res = self._hmget_many([f"customers:{i}" for i in p["ids"]], ["id", "tier"])
        return [(int(r[0]), r[1]) for r in res if r[0] is not None]

    def op_insert_one(self, p):
        self.put("events", p)
        return None

    def op_update_field(self, p):
        self.raw.hset(f"customers:{p['id']}", "last_login", p["value"])
        return None

    def op_upsert(self, p):
        self.put("inventory", {"product_id": p["product_id"], "warehouse_id": p["warehouse_id"], "qty": p["qty"],
                               "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "version": 1})
        return None

    def op_delete_one(self, p):
        self.delete("events", p["id"])
        return None

    def op_atomic_increment(self, p):
        return [(self.raw.hincrby(inv_key(p["product_id"], p["warehouse_id"]), "qty", 1),)]

    def op_conditional_update(self, p):
        k = inv_key(p["product_id"], p["warehouse_id"])
        cur = self.raw.hget(k, "version")
        if cur is None:
            return [(0, -1)]
        try:
            res = self.raw.eval(_CAS_LUA, 1, k, cur, "250")
            return [(int(res[0]), int(res[1]))]
        except ResponseError as e:
            if "unknown command" not in str(e).lower() and "not supported" not in str(e).lower():
                raise
        # fallback: WATCH / MULTI / EXEC optimistic transaction
        with self.raw.pipeline(transaction=True) as tx:
            tx.watch(k)
            v = tx.hget(k, "version")
            if v != cur:
                tx.unwatch()
                return [(0, int(v))]
            tx.multi()
            tx.hset(k, "qty", 250)
            tx.hincrby(k, "version", 1)
            out = tx.execute()
            return [(1, int(out[-1]))]

    def op_batch_insert_1000(self, p):
        self.bulk_put("events", p["docs"])
        return None

    def op_range_by_secondary(self, p):
        ids = sorted(int(x) for x in self.raw.smembers(f"idx:orders:cust:{p['customer_id']}"))
        res = self._hmget_many([f"orders:{i}" for i in ids], ["id", "status", "total_amount"])
        return [(int(r[0]), r[1], float(r[2])) for r in res if r[0] is not None]

    def op_range_by_time(self, p):
        if not self.raw.exists(f"idx:events:type:{p['event_type']}"):
            return self._range_by_time_scan(p)
        lo, hi = score_of(p["start"], 0), score_of(p["end"], 0) - 1
        ids = self.raw.zrangebyscore(f"idx:events:type:{p['event_type']}", lo, hi, start=0, num=p["limit"])
        res = self._hmget_many([f"events:{i}" for i in ids], ["id", "customer_id", "occurred_at"])
        return [(int(r[0]), int(r[1]), r[2]) for r in res if r[0] is not None]

    def _range_by_time_scan(self, p):
        """No index: SCAN the whole keyspace for events:* and filter client-side (what a KV store costs without one)."""
        lo, hi = iso(p["start"]), iso(p["end"])
        out = []
        cursor = 0
        node_iter = self.raw.scan_iter(match="events:*", count=5000) if not self.cluster else self.raw.scan_iter(match="events:*", count=5000)
        for chunk in batched(node_iter, 2000):
            res = self._hmget_many(chunk, ["id", "customer_id", "event_type", "occurred_at"])
            for r in res:
                if r[2] == p["event_type"] and lo <= r[3] < hi:
                    out.append((int(r[0]), int(r[1]), r[3]))
        out.sort(key=lambda t: (t[2], t[0]))
        return out[:p["limit"]]

    def op_filter_nested(self, p):
        ids = self.raw.smembers("idx:customers:tier:pro")
        res = self._hmget_many([f"customers:{i}" for i in ids], ["attrs.lang"])
        return [(sum(1 for r in res if r[0] == "tr"),)]

    def op_array_contains(self, p):
        ids = self.raw.smembers("idx:products:tag:eco")
        res = self._hmget_many([f"products:{i}" for i in ids], ["active"])
        return [(sum(1 for r in res if r[0] == "1"),)]

    def op_top_n_sorted(self, p):
        cand = self.raw.zrevrange(f"idx:products:cat:{p['category_id']}", 0, -1, withscores=True)
        res = self._hmget_many([f"products:{i}" for i, _ in cand], ["active"])
        rows = [(int(i), float(s)) for (i, s), r in zip(cand, res) if r[0] == "1"]
        rows.sort(key=lambda t: (-t[1], t[0]))
        return rows[:p["n"]]

    def op_pagination_offset(self, p):
        ids = self.raw.zrange("idx:orders:by_time", p["offset"], p["offset"] + p["page"] - 1)
        res = self._hmget_many([f"orders:{i}" for i in ids], ["id", "ordered_at"])
        return [(int(r[0]), r[1]) for r in res]

    def op_pagination_keyset(self, p):
        # cursor = score of the last row of the previous page (found once, cached on the client)
        cur = getattr(self, "_cursor", None)
        if cur is None:
            last = self.raw.zrange("idx:orders:by_time", p["offset"] - 1, p["offset"] - 1, withscores=True)
            cur = self._cursor = last[0][1]
        ids = self.raw.zrangebyscore("idx:orders:by_time", f"({cur}", "+inf", start=0, num=p["page"])
        res = self._hmget_many([f"orders:{i}" for i in ids], ["id", "ordered_at"])
        return [(int(r[0]), r[1]) for r in res]

    def op_count_filtered(self, p):
        return [(int(self.raw.scard("idx:orders:status:pending")),)]

    def op_aggregate_group(self, p):
        """Client-side aggregation: a KV store has no GROUP BY; stream every order through a pipeline."""
        keys = self.raw.smembers("keys:orders")
        agg: dict[str, list] = {}
        for chunk in batched(keys, 5000):
            for r in self._hmget_many(list(chunk), ["shipping_country", "total_amount"]):
                if r[0] is None:
                    continue
                a = agg.setdefault(r[0], [0, 0.0])
                a[0] += 1
                a[1] += float(r[1])
        return [(c, n, round(s, 2)) for c, (n, s) in sorted(agg.items())]

    def op_aggregate_time_bucket(self, p):
        if not self.raw.exists("idx:events:type:checkout"):
            raise Unsupported("no events-by-type index (KV store: full keyspace scan needed)")
        ids = self.raw.zrange("idx:events:type:checkout", 0, -1)
        agg: dict[str, list] = {}
        for chunk in batched(ids, 5000):
            for r in self._hmget_many([f"events:{i}" for i in chunk], ["occurred_at", "value_num"]):
                if r[0] is None:
                    continue
                a = agg.setdefault(r[0][:7], [0, 0.0])
                a[0] += 1
                a[1] += float(r[1])
        return [(m, n, round(s, 4)) for m, (n, s) in sorted(agg.items())]

    def op_join_lookup(self, p):
        ids = sorted(int(x) for x in self.raw.smembers(f"idx:orders:cust:{p['customer_id']}"))
        pl = self.pipe()
        pl.hget(f"customers:{p['customer_id']}", "name")
        for i in ids:
            pl.hmget(f"orders:{i}", ["id", "total_amount"])
        res = pl.execute()
        name = res[0]
        return [(int(r[0]), name, float(r[1])) for r in res[1:] if r[0] is not None]

    def op_graph_traversal(self, p):
        cat = self.raw.hget(f"products:{p['product_id']}", "category_id")
        path = []
        seen = 0
        while cat and seen < 10:
            cid = int(cat)
            r = self.raw.hmget(f"categories:{cid}", ["depth", "parent_id"])
            path.append((int(r[0]), cid))
            cat = r[1]
            seen += 1
        return sorted(path)

    def op_text_search(self, p):
        if self.search:
            q = " ".join(p["words"])
            try:
                res = self.raw.execute_command("FT.SEARCH", "ft:products", f"@description:({q})", "NOCONTENT", "LIMIT", 0, 1000)
                ids = sorted(int(str(k).split(":")[-1]) for k in res[1:])
                return [(i,) for i in ids[:p["limit"]]]
            except ResponseError as e:
                if "unknown command" not in str(e).lower():
                    raise
        # no text index: client-side scan of the 5k product descriptions
        keys = self.raw.smembers("keys:products")
        words = [w.lower() for w in p["words"]]
        out = []
        for chunk in batched(keys, 2000):
            for r in self._hmget_many(list(chunk), ["id", "description"]):
                if r[1] and all(w in r[1] for w in words):
                    out.append(int(r[0]))
        return [(i,) for i in sorted(out)[:p["limit"]]]

    def op_geo_near(self, p):
        res = self.raw.geosearch("geo:customers", longitude=p["lon"], latitude=p["lat"], radius=p["km"], unit="km", sort="ASC", count=p["limit"])
        return [(int(x),) for x in res]

    def op_vector_knn(self, p):
        if not self.search:
            raise Unsupported("vector search needs the RediSearch module (Redis 8 / Redis Stack)")
        blob = struct.pack(f"{VEC_DIM}f", *p["vector"])
        res = self.raw.execute_command("FT.SEARCH", "ft:products", f"*=>[KNN {p['k']} @embedding $vec AS score]", "PARAMS", 2, "vec", blob,
                                       "SORTBY", "score", "RETURN", 1, "score", "DIALECT", 2, "LIMIT", 0, p["k"])
        return [(int(str(k).split(":")[-1]),) for k in res[1::2]]

    def op_transaction_transfer(self, p):
        a, b = inv_key(p["product_id"], p["from"]), inv_key(p["product_id"], p["to"])
        with self.raw.pipeline(transaction=True) as tx:
            tx.hincrby(a, "qty", -p["amount"])
            tx.hincrby(b, "qty", p["amount"])
            tx.execute()
        return None

    def op_ttl_set(self, p):
        self.raw.set(f"scratch:{p['key']}", json.dumps(p["value"]), ex=p["seconds"])
        return None

    def op_scan_full(self, p):
        n = 0
        for _ in self.raw.scan_iter(match="events:*", count=10000):
            n += 1
        return [(n,)]


    # --- optimisation hooks (phases/optimize.py) -----------------------------------------------
    def opt_fetch_full(self, coll, key):
        return [tuple(sorted(self.raw.hgetall(f"{coll}:{key}").items()))]

    def opt_fetch_projected(self, coll, key, fields):
        return [tuple(self.raw.hmget(f"{coll}:{key}", fields))]

    def opt_order_embedded(self, order_id):
        d = self.raw.hgetall(f"orders:{order_id}")
        items = json.loads(d.get("items", "[]"))
        return [(int(d["id"]), len(items))]

    def opt_order_referenced(self, order_id):
        pl = self.pipe()
        pl.hmget(f"orders:{order_id}", ["id", "status", "total_amount"])
        pl.smembers(f"idx:items:order:{order_id}")
        head, ids = pl.execute()
        items = self._hmget_many([f"order_items:{i}" for i in ids], ["id", "product_id", "qty"])
        return [(int(head[0]), len(items))]

    def opt_write_durable(self, level, doc):
        k = key_of("events", doc)
        if level == "ack_primary":
            self.raw.hset(k, mapping=to_hash(doc))
        elif level == "wait_1_replica":
            self.raw.hset(k, mapping=to_hash(doc))
            n = self.raw.wait(1, 2000)
            if n < 1:
                raise RuntimeError("WAIT: no replica acknowledged")
        elif level == "waitaof_local_fsync":
            self.raw.hset(k, mapping=to_hash(doc))
            self.raw.execute_command("WAITAOF", 1, 0, 2000)
        elif level == "waitaof_local+1_replica":
            self.raw.hset(k, mapping=to_hash(doc))
            self.raw.execute_command("WAITAOF", 1, 1, 2000)
        return None

    def opt_read_level(self, level, key):
        return self.op_get_by_key({"id": key})

    # --- capability probes ------------------------------------------------------------------------
    STATIC_PROBES = {
        "secondary_index": ("unsupported", "no server-side secondary indexes; application-maintained sets/zsets (or RediSearch FT.CREATE)"),
        "compound_index": ("unsupported", "application-maintained composite keys / zset scores"),
        "unique_constraint": ("unsupported", "keys are unique by definition; no constraint on hash fields (SETNX/HSETNX for keys)"),
        "aggregation": ("unsupported", "no GROUP BY; client-side, Lua, or FT.AGGREGATE with RediSearch"),
        "server_side_join": ("unsupported", "no joins; client-side or Lua"),
        "schema_validation": ("unsupported", "schemaless"),
        "bulk_import": ("supported", "pipelining / redis-cli --pipe (RESP protocol file)"),
    }

    def probe_ttl_expiry(self):
        self.raw.set("scratch:probe_ttl", "1", ex=1)
        time.sleep(1.3)
        if self.raw.exists("scratch:probe_ttl"):
            raise RuntimeError("key still present after TTL")
        return "SET EX / EXPIRE; hash-field TTL (HEXPIRE) in Redis 7.4+/Valkey 9"

    def probe_text_index(self):
        if not self.search:
            raise Unsupported("needs RediSearch module")
        self.raw.execute_command("FT.SEARCH", "ft:products", "@description:(wireless charger)", "NOCONTENT", "LIMIT", 0, 1)
        return "FT.CREATE ... TEXT + FT.SEARCH"

    def probe_geo_index(self):
        self.raw.geosearch("geo:customers", longitude=13.4, latitude=52.5, radius=50, unit="km", count=1)
        return "GEOADD + GEOSEARCH (geohash in a sorted set)"

    def probe_vector_index(self):
        if not self.search:
            raise Unsupported("needs RediSearch module")
        self.op_vector_knn({"vector": [0.1] * VEC_DIM, "k": 1})
        return "FT.CREATE ... VECTOR HNSW + FT.SEARCH KNN"

    def probe_partial_update(self):
        self.raw.hset("customers:1", "probe", "x")
        self.raw.hdel("customers:1", "probe")
        return "HSET one field of a hash"

    def probe_atomic_increment(self):
        v = self.raw.hincrby("scratch:probe_ctr", "n", 1)
        self.raw.delete("scratch:probe_ctr")
        return f"HINCRBY -> {v}"

    def probe_optimistic_concurrency(self):
        self.raw.hset("scratch:probe_cas", mapping={"version": 1, "qty": 1})
        try:
            self.raw.eval(_CAS_LUA, 1, "scratch:probe_cas", "1", "5")
            return "Lua compare-and-set (EVAL) or WATCH/MULTI/EXEC; Valkey 9: SET IFEQ"
        except ResponseError as e:
            if "unknown command" in str(e).lower():
                with self.raw.pipeline(transaction=True) as tx:
                    tx.watch("scratch:probe_cas")
                    tx.multi()
                    tx.hset("scratch:probe_cas", "qty", 5)
                    tx.execute()
                return "WATCH/MULTI/EXEC optimistic transaction (no Lua)"
            raise
        finally:
            self.raw.delete("scratch:probe_cas")

    def probe_multi_doc_transaction(self):
        self.raw.set("scratch:tx_a", 1)
        self.raw.set("scratch:tx_b", 1)
        with self.raw.pipeline(transaction=True) as tx:
            tx.incr("scratch:tx_a")
            tx.incr("scratch:tx_b")
            tx.execute()
        self.raw.delete("scratch:tx_a", "scratch:tx_b")
        return "MULTI/EXEC: atomic + isolated batch, but no rollback after a failed command (not ACID in the SQL sense)"

    def probe_tunable_consistency(self):
        try:
            n = self.raw.wait(1, 200)
            return f"WAIT numreplicas timeout -> {n} replicas acked (async replication; WAIT gives per-write synchronous ack)"
        except ResponseError as e:
            raise Unsupported(str(e))

    def probe_nested_query(self):
        raise Unsupported("flattened hash fields; filtering is client-side (or RediSearch)")

    def probe_array_operators(self):
        self.raw.rpush("scratch:probe_list", "a", "b")
        n = self.raw.lpos("scratch:probe_list", "b")
        self.raw.delete("scratch:probe_list")
        return f"native list/set types (RPUSH/LPOS/SISMEMBER) rather than arrays inside documents; LPOS -> {n}"

    def probe_graph_traversal(self):
        raise Unsupported("client-side loop over parent pointers")

    def probe_change_streams(self):
        ps = self.raw.pubsub()
        ps.psubscribe("__keyevent@0__:set")
        try:
            self.raw.config_set("notify-keyspace-events", "KEA")
            self.raw.set("scratch:probe_ev", 1)
            t0 = time.time()
            while time.time() - t0 < 2:
                m = ps.get_message(timeout=0.5)
                if m and m.get("type") == "pmessage":
                    return "keyspace notifications (notify-keyspace-events) over Pub/Sub"
            raise RuntimeError("no keyspace notification received")
        finally:
            ps.close()
            self.raw.delete("scratch:probe_ev")

    def probe_server_side_scripting(self):
        v = self.raw.eval("return ARGV[1] .. 'x'", 0, "lua")
        return f"Lua EVAL -> {v}; functions (FUNCTION LOAD) in Redis 7+"

    def probe_pubsub(self):
        ps = self.raw.pubsub()
        ps.subscribe("scratch:chan")
        try:
            self.raw.publish("scratch:chan", "hi")
            t0 = time.time()
            while time.time() - t0 < 2:
                m = ps.get_message(timeout=0.5)
                if m and m.get("type") == "message":
                    return "PUBLISH/SUBSCRIBE (fire-and-forget) + Streams (XADD/XREADGROUP) for durable queues"
            raise RuntimeError("no message")
        finally:
            ps.close()

    def probe_explain_plan(self):
        if self.search:
            try:
                self.raw.execute_command("FT.EXPLAIN", "ft:products", "@description:(wireless)")
                return "FT.EXPLAIN for search queries; plain commands have documented O() complexity, no planner"
            except ResponseError:
                pass
        raise Unsupported("no query planner (every command has a documented complexity); SLOWLOG / COMMANDLOG for diagnostics")

    def probe_large_document(self):
        blob = b"x" * (15 * 1024 * 1024)
        self.raw.set("scratch:big", blob)
        n = len(self.raw.get("scratch:big"))
        self.raw.delete("scratch:big")
        return f"15 MB string round-trip ok (limit: proto-max-bulk-len 512 MB)"

    def probe_many_fields(self):
        self.raw.hset("scratch:wide", mapping={f"f{i}": i for i in range(5000)})
        n = self.raw.hlen("scratch:wide")
        self.raw.delete("scratch:wide")
        return f"hash with {n} fields"

    def probe_read_from_replica(self):
        reps = [t for t in self.cfg.targets if t.role == "replica"]
        if not reps and not self.cluster:
            raise Unsupported("no replica in this stack")
        if self.cluster:
            self.raw.get("customers:1")   # RedisCluster(read_from_replicas=True) routes READONLY reads
            return "READONLY + read_from_replicas=True on cluster clients"
        c = self.engine.connect(reps[0])
        try:
            c.raw.hget("customers:1", "name")
            return "connect to the replica directly (replica-read-only yes); Sentinel clients expose slave_for()"
        finally:
            c.close()

    def probe_snapshot_backup(self):
        try:
            self.raw.bgsave()
            return "BGSAVE (RDB snapshot) / AOF; Dragonfly: SAVE DF"
        except ResponseError as e:
            if "already in progress" in str(e).lower():
                return "BGSAVE (RDB snapshot)"
            raise

    def probe_slow_query_log(self):
        v = self.raw.config_get("slowlog-log-slower-than")
        self.raw.execute_command("SLOWLOG", "LEN")
        return f"SLOWLOG (in-memory ring, slowlog-log-slower-than={v.get('slowlog-log-slower-than')} µs) + LATENCY MONITOR"

    def probe_audit_log(self):
        self.raw.execute_command("ACL", "LOG", 1)
        return "ACL LOG (denied AUTH / NOPERM attempts, in memory); no persistent statement audit in OSS (MONITOR is a live stream)"

    def probe_runtime_log_level(self):
        cur = self.raw.config_get("loglevel").get("loglevel", "notice")
        self.raw.config_set("loglevel", cur)
        return f"CONFIG SET loglevel (current {cur}), slowlog-*, latency-monitor-threshold apply immediately"

    STATIC_PROBES = dict(STATIC_PROBES, **{
        "structured_logs": ("unsupported", "plain-text server log (pid:role date level message); wrap with the docker log driver / a shipper"),
        "incremental_backup": ("unsupported", "RDB is always a full snapshot; AOF (everysec) is the continuous log between snapshots"),
        "pitr": ("unsupported", "AOF replays to the last fsync only — no timestamp-based replay; copy RDBs at the RPO you need"),
    })


class RespEngine(Engine):
    driver = "resp"

    def apply_features(self, features: dict) -> None:
        self.cluster = bool(features.get("cluster"))
        self.events_index = bool(features.get("events_index", True))
        self.search = bool(features.get("search"))

    def connect(self, target: Target, *, timeout: float = 10.0) -> RespClient:
        kw = dict(host=target.host, port=target.port, socket_timeout=max(timeout, 60), socket_connect_timeout=timeout,
                  decode_responses=False, protocol=2)
        if target.password:
            kw["password"] = target.password
        if target.user:
            kw["username"] = target.user
        if self.cluster and target.extra.get("cluster", True) and target.role != "replica":
            raw = redis.RedisCluster(**kw, read_from_replicas=bool(target.extra.get("read_from_replicas", False)),
                                     require_full_coverage=False)
        else:
            raw = redis.Redis(**kw)
        c = RespClient(self, target, raw)
        c.raw = _DecodingProxy(raw)
        return c

    def reset(self, client: RespClient) -> None:
        client.flush_all()          # one keyspace for every collection: flushing is the cheap, exact reset before a load

    def create_schema(self, client: RespClient) -> list[str]:
        done = ["hash per document, keys:<coll> sets, application-maintained index sets/zsets, GEOADD geo:customers"]
        if self.search:
            try:
                client.raw.execute_command("FT.DROPINDEX", "ft:products")
            except ResponseError:
                pass
            client.raw.execute_command("FT.CREATE", "ft:products", "ON", "HASH", "PREFIX", 1, "products:", "SCHEMA",
                                       "description", "TEXT", "category_id", "NUMERIC", "price", "NUMERIC",
                                       "embedding", "VECTOR", "HNSW", 6, "TYPE", "FLOAT32", "DIM", VEC_DIM, "DISTANCE_METRIC", "COSINE")
            done.append("FT.CREATE ft:products ON HASH PREFIX products: (description TEXT, embedding VECTOR HNSW cosine)")
        return done

    load_workers = 4
    load_batch = 2000
    load_parse_dates = False
    load_method = "pipeline HSET (2000/batch) + index sets"

    def after_load(self, client: RespClient) -> list[str]:
        out = []
        if self.search:
            for _ in range(120):
                info = client.raw.execute_command("FT.INFO", "ft:products")
                d = {info[i]: info[i + 1] for i in range(0, len(info) - 1, 2)}
                if str(d.get("indexing", "0")) in ("0", "0.0"):
                    out.append(f"ft:products indexed {d.get('num_docs')} docs")
                    break
                time.sleep(0.5)
        return out

    def create_index(self, client: RespClient, name: str, coll: str, fields: list[str]) -> str:
        if coll == "events" and fields[0] in ("event_type", "occurred_at"):
            # build the zset index from the data (what an application would do when adding an access path)
            n = 0
            for chunk in batched(client.raw.scan_iter(match="events:*", count=5000), 2000):
                res = client._hmget_many(chunk, ["id", "event_type", "occurred_at"])
                p = client.pipe()
                for r in res:
                    if r[0] is not None:
                        p.zadd(f"idx:events:type:{r[1]}", {r[0]: score_of(r[2], int(r[0]))})
                        n += 1
                p.execute()
            return f"built idx:events:type:* zsets over {n} events (application-maintained index)"
        raise Unsupported("only the events-by-type manual index is modelled")

    def drop_index(self, client: RespClient, name: str, coll: str) -> None:
        keys = [k for k in client.raw.scan_iter(match="idx:events:type:*", count=100)]
        if keys:
            client.raw.delete(*keys) if not self.cluster else [client.raw.delete(k) for k in keys]

    def durability_levels(self) -> list[str]:
        lv = ["ack_primary"]
        if self.cfg.replicas or self.cluster:
            lv.append("wait_1_replica")
        if self.features.get("aof"):
            lv.append("waitaof_local_fsync")
            if self.cfg.replicas:
                lv.append("waitaof_local+1_replica")
        return lv

    def read_levels(self) -> list[tuple[str, str | None]]:
        lv: list[tuple[str, str | None]] = [("primary", None)]
        for r in self.cfg.replicas:
            lv.append(("replica", r.name))
            break
        return lv

    def max_connections(self, client: RespClient) -> int | None:
        try:
            v = client.raw.config_get("maxclients")
            return int(v.get("maxclients")) if v else None
        except Exception:  # noqa: BLE001
            return None

    def replication_status(self, client: RespClient) -> dict[str, Any]:
        info = client._info("replication")
        keep = {k: v for k, v in info.items() if k in ("role", "connected_slaves", "master_repl_offset", "slave_repl_offset",
                                                        "master_link_status", "master_last_io_seconds_ago", "slave_read_only", "master_host")}
        keep["replicas"] = [v for k, v in info.items() if k.startswith("slave") and isinstance(v, dict)][:6]
        return keep

    def promote(self, target: Target) -> str:
        rep = self.cfg.replication or {}
        method = rep.get("promote", {}).get("method", "engine")
        if method == "sentinel":
            s = self.cfg.by_name(rep["promote"].get("target", "sentinel"))
            r = redis.Redis(host=s.host, port=s.port, socket_timeout=5)
            r.execute_command("SENTINEL", "FAILOVER", rep.get("master_name", "mymaster"))
            return f"SENTINEL FAILOVER {rep.get('master_name', 'mymaster')} via {s.name}"
        c = self.connect(target)
        try:
            c.raw.execute_command("REPLICAOF", "NO", "ONE")
            return f"REPLICAOF NO ONE on {target.name}"
        finally:
            c.close()


    # --- backup / restore ------------------------------------------------------------------------------
    BACKUP_STRATEGY = {
        "tool": "BGSAVE (fork, point-in-time RDB) then copy dump.rdb off the box; AOF everysec keeps the gap between snapshots small",
        "consistency": "point-in-time (fork + copy-on-write); one file per instance",
        "restore": "offline: stop, install the RDB as the multipart-AOF base file (appendonlydir/ + manifest) so an AOF-enabled server loads it, start; replicas full-resync",
        "incremental": "none — RDB is always full; BGREWRITEAOF compacts the AOF",
        "pitr": "no (AOF replays to the last fsync only)",
        "schedule": "`save <seconds> <changes>` rules or a cron job calling BGSAVE + copy (Sentinel/replica-aware)",
        "retention": "external: rotate the copied RDB files",
    }

    def _mount_volume(self, container: str, dest: str) -> str:
        for m in dockerctl.inspect(container).get("Mounts", []):
            if m.get("Destination") == dest and m.get("Name"):
                return m["Name"]
        raise EngineError(f"{container}: no named volume mounted at {dest}")

    def data_bytes(self, client: RespClient) -> int | None:
        t = self.cfg.primary
        return dockerctl.du_bytes(t.container, "/data") if t.container else None

    def backup(self, client: RespClient, tag: str, *, log=print) -> dict[str, Any]:
        t = self.cfg.primary
        if not t.container:
            raise Unsupported("no container for the primary (managed service): use `redis-cli --rdb` over the replication protocol")
        info0 = client._info("persistence")
        t0 = time.time()
        try:
            client.raw.execute_command("BGSAVE")
        except ResponseError as e:
            if "already in progress" not in str(e).lower():
                raise
        fork_us = None
        while True:
            info = client._info("persistence")
            if int(info.get("rdb_bgsave_in_progress", 0)) == 0 and int(info.get("rdb_last_save_time", 0)) >= int(t0) - 1 \
                    and info.get("rdb_last_bgsave_status") == "ok" and int(info.get("rdb_last_save_time", 0)) != int(info0.get("rdb_last_save_time", -1)):
                break
            if time.time() - t0 > 600:
                raise EngineError("BGSAVE did not finish within 600 s")
            time.sleep(0.05)
        try:
            fork_us = int(client._info("stats").get("latest_fork_usec", 0))
        except Exception:  # noqa: BLE001
            pass
        rc, out, err = dockerctl.exec_in(t.container, ["cp", "/data/dump.rdb", f"/backups/{tag}.rdb"], user="0")
        if rc != 0:
            raise EngineError(f"copy dump.rdb: {err[:200]}")
        size = dockerctl.du_bytes(t.container, f"/backups/{tag}.rdb")
        return {"method": "BGSAVE + copy dump.rdb", "online": True, "artifact": f"/backups/{tag}.rdb", "artifact_bytes": size,
                "fork_ms": round(fork_us / 1000, 1) if fork_us else None, "rdb_last_cow_size": info.get("rdb_last_cow_size"),
                "aof_current_size": info.get("aof_current_size"), "used_memory": client._info("memory").get("used_memory")}

    def artifact_paths(self, tag: str) -> list[tuple[str, str]]:
        return [(self.cfg.primary.container, f"/backups/{tag}.rdb")]

    def _sentinels(self) -> list[str]:
        return [c for c in self.cfg.containers if "sentinel" in c]

    def _sentinel_set(self, key: str, value: str) -> dict[str, str]:
        name = (self.cfg.replication or {}).get("master_name", "mymaster")
        prev: dict[str, str] = {}
        for c in self._sentinels():
            rc, out, _ = dockerctl.exec_in(c, ["redis-cli", "-p", "26379", "SENTINEL", "MASTER", name])
            toks = out.split()
            for i in range(0, len(toks) - 1, 2):
                if toks[i] == key:
                    prev[c] = toks[i + 1]
            dockerctl.exec_in(c, ["redis-cli", "-p", "26379", "SENTINEL", "SET", name, key, value])
        return prev

    def restore(self, client: RespClient, tag: str, info: dict[str, Any], *, log=print) -> dict[str, Any]:
        t = self.cfg.primary
        if not t.container:
            raise Unsupported("no container for the primary")
        data_vol = self._mount_volume(t.container, "/data")
        bk_vol = self._mount_volume(t.container, "/backups")
        image = dockerctl.inspect(t.container).get("Config", {}).get("Image") or self.cfg.image
        prev = self._sentinel_set("down-after-milliseconds", "600000") if self._sentinels() else {}
        try:
            t_stop = time.perf_counter()
            dockerctl.container_action(t.container, "stop")
            script = (f"rm -rf /data/appendonlydir /data/dump.rdb && mkdir -p /data/appendonlydir && "
                      f"cp /backups/{tag}.rdb /data/appendonlydir/appendonly.aof.1.base.rdb && "
                      f"printf 'file appendonly.aof.1.base.rdb seq 1 type b\\n' > /data/appendonlydir/appendonly.aof.manifest && "
                      f"chown -R redis:redis /data/appendonlydir")
            rc, out, err = dockerctl.run_oneshot(image, ["sh", "-c", script], volumes={data_vol: "/data", bk_vol: "/backups"})
            if rc != 0:
                raise EngineError(f"install RDB as AOF base: {err[:300]}")
            dockerctl.container_action(t.container, "start")
            dockerctl.wait_for(lambda: self.connect(t, timeout=2).ping(), timeout=120, desc="primary back")
            downtime = round((time.perf_counter() - t_stop) * 1000, 1)
            resync: dict[str, Any] = {}
            for r in self.cfg.replicas:
                t1 = time.perf_counter()
                def _synced(r=r):
                    c = self.connect(r, timeout=2)
                    try:
                        i = c._info("replication")
                        return i.get("master_link_status") == "up" and int(i.get("master_sync_in_progress", 1)) == 0
                    finally:
                        c.close()
                try:
                    dockerctl.wait_for(_synced, timeout=300, desc=f"{r.name} resync")
                    resync[r.name] = round((time.perf_counter() - t1) * 1000, 1)
                except Exception as e:  # noqa: BLE001
                    resync[r.name] = short_err(e)
        finally:
            for c, v in prev.items():
                dockerctl.exec_in(c, ["redis-cli", "-p", "26379", "SENTINEL", "SET", (self.cfg.replication or {}).get("master_name", "mymaster"), "down-after-milliseconds", v])
        return {"method": "stop; RDB -> appendonlydir base + manifest; start; replicas full-resync", "online": False,
                "downtime_ms": downtime, "replica_resync_ms": resync,
                "sentinel": f"down-after-milliseconds raised to 600000 on {len(prev)} sentinels during the restore, then reset" if prev else "n/a"}

    # --- logging -----------------------------------------------------------------------------------------
    LOGGING_STRATEGY = {
        "server_log": "plain text on stdout (`pid:role date level message`), loglevel notice; the docker json-file driver rotates it",
        "slow_query": "SLOWLOG (in-memory ring: slowlog-log-slower-than µs, slowlog-max-len) + LATENCY MONITOR (latency-monitor-threshold ms) + LATENCY DOCTOR",
        "audit": "ACL LOG for denied AUTH / NOPERM attempts (in memory, acllog-max-len); MONITOR is a live debug stream, not a log; persistent audit is Redis Enterprise",
        "runtime": "CONFIG SET loglevel / slowlog-* / latency-monitor-threshold apply immediately (CONFIG REWRITE persists them)",
    }

    def server_log_facts(self, client: RespClient) -> dict[str, Any]:
        t = self.cfg.primary
        out = {"format": "text", "where": "stdout (docker logs) — `logfile` when set", "loglevel": client.raw.config_get("loglevel").get("loglevel")}
        if t.container:
            out["sample"] = [ln[:200] for ln in dockerctl.logs(t.container, tail=3).splitlines()]
        return out

    def slow_query_setup(self, client: RespClient, threshold_ms: int) -> str:
        client.raw.config_set("slowlog-log-slower-than", threshold_ms * 1000)
        client.raw.execute_command("SLOWLOG", "RESET")
        client.raw.execute_command("LATENCY", "RESET")
        return f"CONFIG SET slowlog-log-slower-than {threshold_ms * 1000} (µs) + SLOWLOG RESET; latency-monitor-threshold {client.raw.config_get('latency-monitor-threshold').get('latency-monitor-threshold')} ms"

    def slow_query_fast_op(self, client: RespClient) -> str:
        client.raw.hget("customers:1", "name")
        return "HGET customers:1 name"

    def slow_query_slow_op(self, client: RespClient) -> str:
        client.raw.keys("customers:*")
        return "KEYS customers:* (scans the whole keyspace — the anti-pattern SLOWLOG exists to catch)"

    def slow_query_entries(self, client: RespClient, threshold_ms: int) -> list[dict[str, Any]]:
        out = []
        for e in client.raw.execute_command("SLOWLOG", "GET", 10):
            args = e[3] if len(e) > 3 else []
            out.append({"id": e[0], "ts": e[1], "ms": round(int(e[2]) / 1000, 2), "text": " ".join(str(a) for a in args)[:200],
                        "client": str(e[4]) if len(e) > 4 else None})
        try:
            lat = client.raw.execute_command("LATENCY", "LATEST")
            if lat:
                out.append({"ms": round(float(lat[0][2]), 1), "text": f"LATENCY LATEST event={lat[0][0]} latest={lat[0][2]} ms max={lat[0][3]} ms", "kind": "latency-monitor"})
        except Exception:  # noqa: BLE001
            pass
        return out

    def slow_query_teardown(self, client: RespClient) -> None:
        client.raw.config_set("slowlog-log-slower-than", 10000)

    def audit_log_setup(self, client: RespClient) -> str:
        client.raw.execute_command("ACL", "LOG", "RESET")
        client.raw.execute_command("ACL", "SETUSER", "nslab_auditor", "on", ">s3cret", "~*", "+@read")
        return "ACL LOG RESET; ACL SETUSER nslab_auditor on >s3cret ~* +@read (read-only user to provoke denials)"

    def audit_log_exercise(self, client: RespClient) -> list[str]:
        t = self.cfg.primary
        done = []
        r = redis.Redis(host=t.host, port=t.port, socket_timeout=5, retry_on_error=[], retry=None)
        try:
            try:
                r.execute_command("AUTH", "nslab_auditor", "wrong-password")
            except Exception as e:  # noqa: BLE001  (redis-py raises AuthenticationError, a ConnectionError subclass)
                done.append(f"AUTH nslab_auditor <wrong> -> {type(e).__name__}: {str(e)[:60]}")
            try:
                r.execute_command("AUTH", "nslab_auditor", "s3cret")
                r.set("audit:probe", "1")
            except Exception as e:  # noqa: BLE001
                done.append(f"SET as read-only nslab_auditor -> {type(e).__name__}: {str(e)[:60]}")
        finally:
            r.close()
        return done

    def audit_log_entries(self, client: RespClient) -> list[str]:
        out = []
        for e in client.raw.execute_command("ACL", "LOG", 10):
            d = {str(e[i]): e[i + 1] for i in range(0, len(e) - 1, 2)} if isinstance(e, list) else e
            if isinstance(d, dict):
                out.append(f"reason={d.get('reason')} context={d.get('context')} object={d.get('object')} user={d.get('username')} count={d.get('count')}")
            else:
                out.append(str(d)[:200])
        return out

    def audit_log_teardown(self, client: RespClient) -> None:
        client.raw.execute_command("ACL", "DELUSER", "nslab_auditor")

    def log_level_runtime(self, client: RespClient) -> str:
        cur = client.raw.config_get("loglevel").get("loglevel", "notice")
        client.raw.config_set("loglevel", "debug")
        seen = client.raw.config_get("loglevel").get("loglevel")
        client.raw.config_set("loglevel", cur)
        if seen != "debug":
            raise EngineError(f"loglevel did not change (got {seen})")
        return f"CONFIG SET loglevel debug -> verified -> back to {cur} (no restart)"


class _DecodingProxy:
    """redis-py client with decode_responses=False (needed for binary vectors) but str results for everything else."""

    def __init__(self, raw):
        self._raw = raw

    def __getattr__(self, name):
        attr = getattr(self._raw, name)
        if not callable(attr):
            return attr

        def wrapped(*a, **kw):
            return _dec(attr(*a, **kw))
        return wrapped

    def pipeline(self, transaction=False):
        return _PipeProxy(self._raw.pipeline(transaction=transaction))


class _PipeProxy:
    def __init__(self, p):
        self._p = p

    def __getattr__(self, name):
        return getattr(self._p, name)

    def __enter__(self):
        self._p.__enter__()
        return self

    def __exit__(self, *a):
        return self._p.__exit__(*a)

    def execute(self, *a, **kw):
        return _dec(self._p.execute(*a, **kw))


def _dec(v):
    if isinstance(v, bytes):
        try:
            return v.decode()
        except UnicodeDecodeError:
            return v
    if isinstance(v, list):
        return [_dec(x) for x in v]
    if isinstance(v, tuple):
        return tuple(_dec(x) for x in v)
    if isinstance(v, dict):
        return {_dec(k): _dec(x) for k, x in v.items()}
    if isinstance(v, set):
        return {_dec(x) for x in v}
    return v
