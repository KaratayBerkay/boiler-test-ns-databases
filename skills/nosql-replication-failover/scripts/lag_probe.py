#!/usr/bin/env python3
"""Measure primary->replica visibility lag on a running nslab stack.

Usage: uv run --project harness python skills/nosql-replication-failover/scripts/lag_probe.py <stack> [--replica NAME] [--n 30]
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "harness"))
from nslab.config import load_stack               # noqa: E402
from nslab.engines import make_engine             # noqa: E402
from nslab.phases.replication import visibility_lag, PROBE  # noqa: E402
from nslab.engines.base import Engine              # noqa: E402


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("stack"); ap.add_argument("--replica", default=None); ap.add_argument("--n", type=int, default=30)
    a = ap.parse_args()
    cfg = load_stack(a.stack); eng = make_engine(cfg)
    primary = cfg.by_name((cfg.replication or {}).get("write_target")) or cfg.primary
    reps = cfg.replicas or [t for t in cfg.nodes if t is not primary]
    rep = cfg.by_name(a.replica) if a.replica else (reps[0] if reps else None)
    if rep is None:
        print(f"{a.stack}: no replica/secondary configured"); return
    # ensure the probe collection exists
    c = eng.connect(primary)
    try:
        try: c.drop_collection(PROBE)
        except Exception: pass
        if hasattr(eng, "create_probe_collection"): eng.create_probe_collection(c)
    finally:
        c.close()
    v = visibility_lag(eng, primary, rep, n=a.n)
    print(f"{a.stack} primary={primary.name} -> replica={rep.name}: visible {v['visible']}/{v['n']} timeouts={v['timeouts']} "
          f"lag p50={v['lag_ms'].get('p50',0):.3f} p95={v['lag_ms'].get('p95',0):.3f} max={v['lag_ms'].get('max',0):.3f} ms")


if __name__ == "__main__":
    main()
