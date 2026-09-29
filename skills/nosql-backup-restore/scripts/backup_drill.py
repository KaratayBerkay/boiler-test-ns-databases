#!/usr/bin/env python3
"""Backup → mutate → restore → verify drill against a running nslab stack (the backup phase, standalone).

Usage: uv run --project harness python skills/nosql-backup-restore/scripts/backup_drill.py <stack> [--json]
Prints backup/restore timings, artifact size vs data size, the point-read latency during the backup, downtime, and
whether the restore was verified (counts, deleted docs back, post-backup writes gone).
"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "harness"))
from nslab.config import load_stack              # noqa: E402
from nslab.engines import make_engine            # noqa: E402
from nslab.phases.backup import run_backup       # noqa: E402


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("stack"); ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    cfg = load_stack(a.stack); eng = make_engine(cfg)
    r = run_backup(eng, cfg, cfg.scale, log=print)
    if a.json:
        print(json.dumps(r, indent=1, default=str)); return
    b, rs = r.get("backup", {}), r.get("restore", {})
    print(f"\n{a.stack}: backup [{b.get('method')}] {b.get('seconds')} s, artifact {b.get('artifact_bytes')} B = {b.get('artifact_pct_of_data')}% of {r.get('data_bytes')} B, "
          f"online={b.get('online')}; restore [{rs.get('method')}] {rs.get('seconds')} s, downtime {(rs.get('availability') or {}).get('downtime_ms')} ms, verified={rs.get('verified')}")


if __name__ == "__main__":
    main()
