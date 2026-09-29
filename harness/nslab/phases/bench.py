"""Phase: timed execution of the operation catalog."""
from __future__ import annotations

import random
from typing import Any

from ..config import StackConfig
from ..datagen import Sizes
from ..engines import Engine
from ..ops import CATALOG, Op, Unsupported
from ..timing import measure
from ..util import short_err


def run_op_timed(client, op: Op, params: list[dict], *, iters: int | None = None, warmup: int | None = None,
                 max_seconds: float | None = None):
    """Time one catalog operation on an open client (params cycled per iteration)."""
    fn_op = getattr(client, f"op_{op.id}")
    it = {"i": 0}

    def fn():
        p = params[it["i"] % len(params)] if params else {}
        it["i"] += 1
        return fn_op(p)

    return measure(op.id, fn, warmup=warmup if warmup is not None else op.warmup, iters=iters if iters is not None else op.iters,
                   max_seconds=max_seconds if max_seconds is not None else op.max_seconds)


def run_bench(engine: Engine, cfg: StackConfig, scale: float, *, only: set[str] | None = None, log=print) -> dict[str, Any]:
    sz = Sizes.for_scale(scale)
    out: dict[str, Any] = {}
    client = engine.connect_primary(timeout=30)
    try:
        for op in CATALOG:
            if only and op.id not in only:
                continue
            rec: dict[str, Any] = {"title": op.title, "tags": list(op.tags), "kind": op.kind}
            if not hasattr(client, f"op_{op.id}"):
                rec["status"] = "unsupported"
                rec["error"] = "no implementation for this engine family"
                out[op.id] = rec
                log(f"  --  {op.id:24s} unsupported")
                continue
            params = op.params(sz, random.Random(11)) if op.params else [{}]
            rec["params_sample"] = str(params[0])[:160]
            if op.setup:
                try:
                    getattr(client, op.setup)(params)
                except Exception as e:  # noqa: BLE001
                    rec["setup_error"] = short_err(e)
            if hasattr(client, "_cursor"):
                del client._cursor          # keyset cursors are computed per op run
            t = run_op_timed(client, op, params)
            rec["status"] = "ok" if t.ok else "error"
            if not t.ok and t.exc is not None and engine.is_unsupported_error(t.exc):
                rec["status"] = "unsupported"
            rec["timing"] = {"n": t.n, **{k: round(v, 3) for k, v in t.stats.items() if k != "n"}, "first_ms": round(t.first_ms, 3) if t.first_ms else None}
            rec["rows"] = t.rows if op.expect_rows else None
            rec["checksum"] = t.checksum if op.expect_rows else None
            rec["error"] = t.error
            rec["error_code"] = engine.error_code(t.exc) if t.exc is not None else None
            meta = getattr(client, "op_meta", {}).get(op.id) if hasattr(client, "op_meta") else None
            if meta:
                rec["meta"] = meta
            if op.cleanup:
                try:
                    getattr(client, op.cleanup)(params)
                except Exception as e:  # noqa: BLE001
                    rec["cleanup_error"] = short_err(e)
            out[op.id] = rec
            if t.ok:
                log(f"  OK  {op.id:24s} p50={t.stats['p50']:9.3f} ms  p95={t.stats['p95']:9.3f} ms  rows={t.rows if op.expect_rows else '-'}")
            elif rec["status"] == "unsupported":
                log(f"  --  {op.id:24s} unsupported ({(t.error or '')[:90]})")
            else:
                log(f"  ERR {op.id:24s} {(t.error or '')[:110]}")
    finally:
        client.close()
    return out
