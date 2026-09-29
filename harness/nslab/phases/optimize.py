"""Phase: optimisation experiments — measure an operation, change one thing (index, document shape,
batching, consistency level, connection handling), measure again.

Engine adapters take part through optional hooks; an experiment whose hook is missing is reported as n/a:
  Engine.create_index / drop_index                      opt01
  Client.opt_fetch_full / opt_fetch_projected           opt02
  Client.op_pagination_offset / op_pagination_keyset    opt03
  Client.put / bulk_put                                 opt04
  Client.op_get_by_key / op_multi_get                   opt05
  Client.opt_order_embedded / opt_order_referenced      opt06
  Engine.durability_levels + Client.opt_write_durable   opt07
  Engine.read_levels + Client.opt_read_level            opt08
  Engine.connect (per op) vs one client                 opt09
"""
from __future__ import annotations

import datetime as dt
import random
import time
from typing import Any, Callable

from ..config import StackConfig
from ..datagen import Sizes
from ..engines import Engine
from ..ops import OP_BY_ID, Unsupported
from ..timing import measure
from ..util import short_err


def _timed(name: str, fn: Callable[[], Any], *, warmup=2, iters=10, max_seconds=20.0) -> dict[str, Any]:
    t = measure(name, fn, warmup=warmup, iters=iters, max_seconds=max_seconds)
    rec: dict[str, Any] = {"name": name, "ok": t.ok, "error": t.error}
    if t.ok:
        rec.update({"p50_ms": round(t.stats.get("p50", 0), 3), "p95_ms": round(t.stats.get("p95", 0), 3), "n": t.n, "rows": t.rows})
    if not t.ok and t.exc is not None and isinstance(t.exc, (Unsupported, NotImplementedError)):
        rec["status"] = "n/a"
    return rec


def _cycle(params: list[dict]):
    i = {"n": 0}

    def nxt():
        p = params[i["n"] % len(params)]
        i["n"] += 1
        return p
    return nxt


def _summary(rec: dict[str, Any]) -> None:
    oks = [v for v in rec["variants"] if v.get("ok") and v.get("p50_ms")]
    if len(oks) >= 2:
        base = oks[0]["p50_ms"]
        best = min(oks, key=lambda x: x["p50_ms"])
        rec["best_variant"] = best["name"]
        rec["speedup_vs_first"] = round(base / best["p50_ms"], 2) if best["p50_ms"] else None


def _log_variant(log, v: dict[str, Any]) -> None:
    if v.get("ok"):
        log(f"    {v['name']:36s} p50={v['p50_ms']:9.3f} ms" + (f" rows={v['rows']}" if v.get("rows") is not None else "") + (f"  [{v['note']}]" if v.get("note") else ""))
    elif v.get("status") == "n/a" or v.get("rows_per_s") is None and "rows_per_s" in v:
        log(f"    {v['name']:36s} n/a ({(v.get('error') or '')[:80]})")
    else:
        log(f"    {v['name']:36s} ERR {(v.get('error') or '')[:90]}")


def exp_secondary_index(engine: Engine, client, sz: Sizes, log) -> dict[str, Any]:
    op = OP_BY_ID["range_by_time"]
    params = op.params(sz, random.Random(11))
    rec: dict[str, Any] = {"title": "Range scan on events(event_type, occurred_at): no index vs secondary index (both column orders)",
                           "what": "an access path that is not indexed is a full scan; composite order matters on B-tree-like engines", "variants": []}
    nxt = _cycle(params)
    variants = getattr(engine, "index_variants", lambda: [("no_index", None), ("index_(event_type,occurred_at)", ["event_type", "occurred_at"]),
                                                         ("index_(occurred_at,event_type)", ["occurred_at", "event_type"])])()
    for name, fields in variants:
        v: dict[str, Any] = {"name": name}
        try:
            if fields is None:
                engine.drop_index(client, "ix_events_type_ts", "events")
                v["note"] = getattr(engine, "no_index_note", "")
            else:
                v["setup"] = engine.create_index(client, "ix_events_type_ts", "events", fields)
            v.update(_timed(name, lambda: client.op_range_by_time(nxt()), iters=8, max_seconds=40))
        except Unsupported as e:
            v.update({"ok": False, "status": "n/a", "error": str(e)})
        except Exception as e:  # noqa: BLE001
            v.update({"ok": False, "error": short_err(e)})
        finally:
            if fields is not None:
                try:
                    engine.drop_index(client, "ix_events_type_ts", "events")
                except Exception:  # noqa: BLE001
                    pass
        rec["variants"].append(v)
        _log_variant(log, v)
    # restore the baseline index for engines that build one at load time
    try:
        if getattr(engine, "events_index", False):
            engine.create_index(client, "ix_events_type_ts", "events", ["event_type", "occurred_at"])
    except Exception:  # noqa: BLE001
        pass
    _summary(rec)
    return rec


def exp_projection(engine: Engine, client, sz: Sizes, log) -> dict[str, Any]:
    rec: dict[str, Any] = {"title": "Point read of an order: whole document (with embedded items) vs 3-field projection",
                           "what": "returning fewer fields saves serialisation and transfer; some engines still read the whole document", "variants": []}
    r = random.Random(12)
    ids = [r.randint(1, sz.orders) for _ in range(50)]
    nxt = _cycle([{"id": i} for i in ids])
    for name, fn in (("full_document", lambda: client.opt_fetch_full("orders", nxt()["id"])),
                     ("projection_3_fields", lambda: client.opt_fetch_projected("orders", nxt()["id"], ["id", "status", "total_amount"]))):
        v = {"name": name}
        try:
            v.update(_timed(name, fn, iters=30))
        except Exception as e:  # noqa: BLE001
            v.update({"ok": False, "error": short_err(e)})
        rec["variants"].append(v)
        _log_variant(log, v)
    _summary(rec)
    return rec


def exp_pagination(engine: Engine, client, sz: Sizes, log) -> dict[str, Any]:
    rec: dict[str, Any] = {"title": "Deep pagination: offset/skip vs keyset (cursor) at depth 10k and 100k",
                           "what": "offset still walks and discards rows; a keyset cursor seeks directly into the sorted index", "variants": []}
    for depth in (10_000, 100_000):
        for kind in ("offset", "keyset"):
            name = f"{kind}_depth_{depth}"
            v = {"name": name}
            fn = getattr(client, f"op_pagination_{kind}", None)
            if fn is None:
                v.update({"ok": False, "status": "n/a", "error": "not implemented"})
            else:
                if hasattr(client, "_cursor"):
                    del client._cursor
                p = {"offset": depth, "page": 50}
                try:
                    v.update(_timed(name, lambda: fn(p), iters=8, max_seconds=30))
                except Exception as e:  # noqa: BLE001
                    v.update({"ok": False, "error": short_err(e)})
            rec["variants"].append(v)
            _log_variant(log, v)
    _summary(rec)
    return rec


def exp_batching(engine: Engine, client, sz: Sizes, log) -> dict[str, Any]:
    rec: dict[str, Any] = {"title": "Write batching: 2000 events one-by-one vs 2 bulk requests of 1000",
                           "what": "round trips dominate small writes; every engine has a bulk path (insert_many, pipeline, batch, _bulk)", "variants": []}
    r = random.Random(13)
    base = 80_000_000
    for name, batch in (("single_put_x2000", 1), ("bulk_put_1000_x2", 1000)):
        docs = [{"id": base + i, "customer_id": r.randint(1, sz.customers), "event_type": "bench_batching",
                 "occurred_at": dt.datetime(2026, 1, 5), "payload": None, "value_num": 1.0} for i in range(2000)]
        base += 2000
        t0 = time.perf_counter()
        err = None
        try:
            if batch == 1:
                for d in docs:
                    client.put("events", d)
            else:
                for i in range(0, 2000, batch):
                    client.bulk_put("events", docs[i:i + batch])
        except Exception as e:  # noqa: BLE001
            err = short_err(e)
        secs = time.perf_counter() - t0
        v = {"name": name, "rows": 2000, "seconds": round(secs, 3), "rows_per_s": round(2000 / secs) if secs > 0 and not err else None, "error": err, "ok": err is None}
        rec["variants"].append(v)
        log(f"    {name:36s} {v['rows_per_s'] or 0:>9,} docs/s {('ERR ' + err) if err else ''}")
        try:
            client.delete_many("events", [d["id"] for d in docs])
        except Exception:  # noqa: BLE001
            pass
    oks = [v for v in rec["variants"] if v.get("rows_per_s")]
    if len(oks) == 2 and oks[0]["rows_per_s"]:
        rec["speedup_vs_first"] = round(oks[1]["rows_per_s"] / oks[0]["rows_per_s"], 2)
        rec["best_variant"] = max(oks, key=lambda v: v["rows_per_s"])["name"]
    return rec


def exp_multi_get(engine: Engine, client, sz: Sizes, log) -> dict[str, Any]:
    rec: dict[str, Any] = {"title": "100 point reads: sequential round trips vs one multi-get request",
                           "what": "batched reads (MGET / $in / IN / mget / pipelining) amortise the network round trip", "variants": []}
    r = random.Random(14)
    sets = [sorted(r.sample(range(1, sz.customers + 1), 100)) for _ in range(10)]
    nxt = _cycle([{"ids": s} for s in sets])

    def sequential():
        out = []
        for i in nxt()["ids"]:
            out.extend(client.op_get_by_key({"id": i}))
        return out

    for name, fn in (("sequential_100_gets", sequential), ("multi_get_100", lambda: client.op_multi_get(nxt()))):
        v = {"name": name}
        if name == "multi_get_100" and not hasattr(client, "op_multi_get"):
            v.update({"ok": False, "status": "n/a", "error": "no multi-get"})
        else:
            v.update(_timed(name, fn, iters=10))
        rec["variants"].append(v)
        _log_variant(log, v)
    _summary(rec)
    return rec


def exp_embedding(engine: Engine, client, sz: Sizes, log) -> dict[str, Any]:
    rec: dict[str, Any] = {"title": "Order with its items: embedded array (one document) vs referenced (order + items lookup)",
                           "what": "embedding what is read together avoids a second query / join; referencing keeps documents small and items independently addressable", "variants": []}
    r = random.Random(15)
    nxt = _cycle([{"id": r.randint(1, sz.orders)} for _ in range(40)])
    for name, meth in (("embedded_items", "opt_order_embedded"), ("referenced_items", "opt_order_referenced")):
        v = {"name": name}
        fn = getattr(client, meth, None)
        if fn is None:
            v.update({"ok": False, "status": "n/a", "error": "not modelled for this engine"})
        else:
            v.update(_timed(name, lambda: fn(nxt()["id"]), iters=30))
        rec["variants"].append(v)
        _log_variant(log, v)
    _summary(rec)
    return rec


def exp_durability(engine: Engine, client, sz: Sizes, log) -> dict[str, Any]:
    rec: dict[str, Any] = {"title": "Single-document write at each durability / write-concern level the engine offers",
                           "what": "acknowledgement level (replica acks, fsync, refresh) is the biggest lever on write latency", "variants": []}
    levels = getattr(engine, "durability_levels", lambda: [])()
    if not levels:
        rec["status"] = "n/a"
        log("    n/a (no tunable write durability)")
        return rec
    base = 90_000_000
    for lvl in levels:
        counter = {"n": 0}

        def fn(lvl=lvl):
            counter["n"] += 1
            return client.opt_write_durable(lvl, {"id": base + counter["n"], "customer_id": 1, "event_type": "bench_durability",
                                                  "occurred_at": dt.datetime(2026, 1, 6), "payload": None, "value_num": 1.0})
        base += 10_000
        v = {"name": lvl}
        v.update(_timed(lvl, fn, iters=30))
        rec["variants"].append(v)
        _log_variant(log, v)
    try:
        client.delete_many("events", [90_000_000 + k * 10_000 + i for k in range(len(levels)) for i in range(1, 40)])
    except Exception:  # noqa: BLE001
        pass
    _summary(rec)
    return rec


def exp_read_consistency(engine: Engine, cfg: StackConfig, sz: Sizes, log) -> dict[str, Any]:
    rec: dict[str, Any] = {"title": "Point read at each read consistency level / read preference the engine offers",
                           "what": "linearizable / quorum reads cost extra round trips; replica reads trade freshness for offload", "variants": []}
    levels = getattr(engine, "read_levels", lambda: [])()
    if not levels:
        rec["status"] = "n/a"
        log("    n/a (no tunable read consistency)")
        return rec
    r = random.Random(16)
    ids = [r.randint(1, sz.customers) for _ in range(50)]
    for lvl, target_name in levels:
        t = cfg.by_name(target_name) if target_name else cfg.primary
        v = {"name": lvl + (f"@{target_name}" if target_name else "")}
        try:
            c = engine.connect(t)
        except Exception as e:  # noqa: BLE001
            v.update({"ok": False, "error": short_err(e)})
            rec["variants"].append(v)
            _log_variant(log, v)
            continue
        nxt = _cycle([{"id": i} for i in ids])
        try:
            v.update(_timed(v["name"], lambda: c.opt_read_level(lvl, nxt()["id"]), iters=30))
        finally:
            c.close()
        rec["variants"].append(v)
        _log_variant(log, v)
    _summary(rec)
    return rec


def exp_connection_reuse(engine: Engine, cfg: StackConfig, sz: Sizes, log) -> dict[str, Any]:
    rec: dict[str, Any] = {"title": "Point read with a new connection per operation vs one persistent client",
                           "what": "handshakes (TCP, TLS, auth, topology discovery) dwarf a point read; always pool", "variants": []}
    r = random.Random(17)
    nxt = _cycle([{"id": r.randint(1, sz.customers)} for _ in range(50)])

    def per_op():
        c = engine.connect(cfg.primary)
        try:
            return c.op_get_by_key(nxt())
        finally:
            c.close()

    v = {"name": "connect_per_operation"}
    v.update(_timed(v["name"], per_op, iters=20))
    rec["variants"].append(v)
    _log_variant(log, v)
    c = engine.connect(cfg.primary)
    try:
        v = {"name": "persistent_client"}
        v.update(_timed(v["name"], lambda: c.op_get_by_key(nxt()), iters=30))
    finally:
        c.close()
    rec["variants"].append(v)
    _log_variant(log, v)
    _summary(rec)
    return rec


def run_optimize(engine: Engine, cfg: StackConfig, scale: float, *, log=print) -> dict[str, Any]:
    sz = Sizes.for_scale(scale)
    out: dict[str, Any] = {}
    client = engine.connect_primary(timeout=30)
    try:
        for exp_id, fn in (("opt01_secondary_index", lambda: exp_secondary_index(engine, client, sz, log)),
                           ("opt02_projection", lambda: exp_projection(engine, client, sz, log)),
                           ("opt03_pagination", lambda: exp_pagination(engine, client, sz, log)),
                           ("opt04_batching", lambda: exp_batching(engine, client, sz, log)),
                           ("opt05_multi_get", lambda: exp_multi_get(engine, client, sz, log)),
                           ("opt06_embedding_vs_reference", lambda: exp_embedding(engine, client, sz, log)),
                           ("opt07_write_durability", lambda: exp_durability(engine, client, sz, log)),
                           ("opt08_read_consistency", lambda: exp_read_consistency(engine, cfg, sz, log)),
                           ("opt09_connection_reuse", lambda: exp_connection_reuse(engine, cfg, sz, log))):
            log(f"  {exp_id}")
            try:
                out[exp_id] = fn()
            except Exception as e:  # noqa: BLE001
                out[exp_id] = {"error": short_err(e)}
                log(f"    FAILED: {short_err(e)}")
    finally:
        client.close()
    return out
