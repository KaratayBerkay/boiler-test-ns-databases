#!/usr/bin/env python3
"""Read an engine's slow-query log through its own interface (SLOWLOG GET, system.profile + getLog, debug.log
MonitoringTask lines, the Neo4j query collector, the Elasticsearch index slowlog) — optionally after provoking one slow op.

Usage: uv run --project harness python skills/nosql-logging-observability/scripts/slowlog.py <stack> [--threshold-ms 100] [--exercise]
"""
from __future__ import annotations
import argparse, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "harness"))
from nslab.config import load_stack              # noqa: E402
from nslab.engines import make_engine            # noqa: E402


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("stack"); ap.add_argument("--threshold-ms", type=int, default=100); ap.add_argument("--exercise", action="store_true")
    a = ap.parse_args()
    cfg = load_stack(a.stack); eng = make_engine(cfg); c = eng.connect_primary(timeout=30)
    try:
        thr = a.threshold_ms
        if a.exercise:
            st = eng.slow_query_setup(c, thr)
            if isinstance(st, tuple):
                st, thr = st
            print(f"setup: {st}")
            print(f"slow op: {eng.slow_query_slow_op(c)}")
            time.sleep(float(cfg.features.get("slowlog_settle_seconds", 0.5)))
        for e in eng.slow_query_entries(c, thr):
            print(f"{str(e.get('ms')):>10s} ms  {e.get('text')}")
        if a.exercise:
            eng.slow_query_teardown(c)
    finally:
        c.close()


if __name__ == "__main__":
    main()
