#!/usr/bin/env python3
"""Time one operation-catalog op against a running nslab stack, with warmup + percentiles + checksum.

Usage: uv run --project harness python skills/nosql-benchmark-timing/scripts/bench_op.py <stack> <op_id> [--iters N] [--target NAME]
Example: ... bench_op.py redis get_by_key --iters 50
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "harness"))
from nslab.config import load_stack               # noqa: E402
from nslab.engines import make_engine             # noqa: E402
from nslab.datagen import Sizes                    # noqa: E402
from nslab.ops import OP_BY_ID                      # noqa: E402
from nslab.phases.bench import run_op_timed         # noqa: E402
import random                                       # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("stack"); ap.add_argument("op")
    ap.add_argument("--iters", type=int, default=None); ap.add_argument("--target", default="primary")
    ap.add_argument("--scale", type=float, default=None)
    a = ap.parse_args()
    cfg = load_stack(a.stack); eng = make_engine(cfg)
    sz = Sizes.for_scale(a.scale if a.scale is not None else cfg.scale)
    op = OP_BY_ID[a.op]
    t = cfg.by_name(a.target) or cfg.primary
    c = eng.connect(t)
    try:
        if not hasattr(c, f"op_{op.id}"):
            print(f"{a.stack}: {op.id} is unsupported (no adapter method)"); return
        params = op.params(sz, random.Random(11)) if op.params else [{}]
        if op.setup:
            getattr(c, op.setup)(params)
        r = run_op_timed(c, op, params, iters=a.iters)
        if r.ok:
            s = r.stats
            print(f"{a.stack} {op.id}: n={r.n} p50={s['p50']:.3f} p95={s['p95']:.3f} p99={s['p99']:.3f} ms "
                  f"min={s['min']:.3f} first={r.first_ms:.3f} rows={r.rows} checksum={r.checksum}")
        else:
            print(f"{a.stack} {op.id}: ERROR {r.error}")
        if op.cleanup:
            getattr(c, op.cleanup)(params)
    finally:
        c.close()


if __name__ == "__main__":
    main()
