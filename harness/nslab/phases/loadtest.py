"""Phase: multi-connection bulk-insert load test with per-container resource accounting.

For each target and worker count, W processes write `batch`-document bulk requests into `events` for `seconds`.
Reports docs/s, batch latency percentiles, errors, and exact cgroup CPU/memory of every container in the stack.
"""
from __future__ import annotations

import datetime as dt
import multiprocessing as mp
import random
import time
from typing import Any

from .. import dockerctl
from ..config import StackConfig
from ..datagen import Sizes
from ..engines import Engine
from ..util import short_err, summarize


def _insert_worker(stack_key: str, target_name: str, upper: int, seconds: float, batch: int, seed: int, start_at: float, q: mp.Queue, run_index: int = 0) -> None:
    from ..config import load_stack
    from ..engines import make_engine
    cfg = load_stack(stack_key)
    eng = make_engine(cfg)
    t = cfg.by_name(target_name) or cfg.primary
    lat: list[float] = []
    rows = 0
    errors = 0
    first_err = None
    try:
        c = eng.connect(t, timeout=30)
    except Exception as e:  # noqa: BLE001
        q.put({"seed": seed, "lat": [], "rows": 0, "errors": 1, "first_error": f"connect: {e}"[:200]})
        return
    r = random.Random(seed)
    base = 40_000_000_000 + (run_index * 100 + seed) * 10_000_000
    n = 0
    inserted: list[int] = []
    while time.time() < start_at:
        time.sleep(0.001)
    stop = time.perf_counter() + seconds
    while time.perf_counter() < stop:
        docs = [{"id": base + n + i, "customer_id": r.randint(1, upper), "event_type": "loadtest",
                 "occurred_at": dt.datetime(2026, 2, 1, 0, 0, 0), "payload": None, "value_num": float(r.randint(0, 100))} for i in range(batch)]
        n += batch
        t0 = time.perf_counter_ns()
        try:
            c.bulk_put("events", docs)
            lat.append((time.perf_counter_ns() - t0) / 1e6)
            rows += batch
            inserted.extend(d["id"] for d in docs)
        except Exception as e:  # noqa: BLE001
            errors += 1
            first_err = first_err or str(e)[:200]
            if errors > 20:
                break
    # cleanup outside the measured window (each worker removes what it wrote)
    cleanup_s = time.perf_counter()
    try:
        for i in range(0, len(inserted), 5000):
            c.delete_many("events", inserted[i:i + 5000])
    except Exception:  # noqa: BLE001
        pass
    cleanup_s = time.perf_counter() - cleanup_s
    try:
        c.close()
    except Exception:  # noqa: BLE001
        pass
    q.put({"seed": seed, "lat": lat, "rows": rows, "errors": errors, "first_error": first_err, "cleanup_s": round(cleanup_s, 1)})


def run_insert_workers(stack_key: str, target_name: str, upper: int, workers: int, seconds: float, batch: int, run_index: int = 0) -> dict[str, Any]:
    ctx = mp.get_context("fork")
    q: mp.Queue = ctx.Queue()
    start_at = time.time() + min(5.0, 0.05 * workers + 1.5)
    procs = [ctx.Process(target=_insert_worker, args=(stack_key, target_name, upper, seconds, batch, i, start_at, q, run_index), daemon=True) for i in range(workers)]
    for p in procs:
        p.start()
    results = []
    deadline = start_at + seconds + 300
    while len(results) < workers and time.time() < deadline:
        try:
            results.append(q.get(timeout=1.0))
        except Exception:  # noqa: BLE001
            pass
    for p in procs:
        p.join(timeout=5)
        if p.is_alive():
            p.terminate()
    lat = [x for r in results for x in r["lat"]]
    rows = sum(r["rows"] for r in results)
    return {"workers": workers, "target": target_name, "rows": rows, "batches": len(lat), "errors": sum(r["errors"] for r in results),
            "seconds": seconds, "rows_per_s": round(rows / seconds), "batch_ms": summarize(lat),
            "first_error": next((r["first_error"] for r in results if r.get("first_error")), None), "workers_reported": len(results),
            "cleanup_s": max((r.get("cleanup_s", 0) for r in results), default=0)}


def run_loadtest(engine: Engine, cfg: StackConfig, scale: float, *, log=print, targets: list[str] | None = None,
                 workers_list: list[int] | None = None, seconds: float | None = None, batch: int | None = None) -> dict[str, Any]:
    sz = Sizes.for_scale(scale)
    f = cfg.features
    targets = targets or f.get("loadtest_targets") or [cfg.primary.name] + [p.name for p in cfg.proxies if not p.extra.get("splits_reads")]
    workers_list = workers_list or f.get("loadtest_workers") or [4, 16, 32]
    seconds = seconds or float(f.get("loadtest_seconds", 15))
    batch = batch or int(f.get("loadtest_batch", 1000))
    containers = cfg.containers
    out: dict[str, Any] = {"batch_rows": batch, "seconds_per_run": seconds, "containers": containers, "runs": []}
    run_index = int(time.time()) % 1000
    if engine.embedded or not engine.supports_processes:
        return {"status": "n/a", "note": "embedded single-process engine: see optimize opt04 batching for write throughput"}
    for tn in targets:
        t = cfg.by_name(tn)
        if t is None:
            log(f"  unknown target {tn}")
            continue
        for w in workers_list:
            paths = dockerctl.cgroup_paths(containers)
            before = dockerctl.cgroup_snapshot(paths)
            run_index += 1
            r = run_insert_workers(cfg.key, tn, sz.customers, w, seconds, batch, run_index)
            after = dockerctl.cgroup_snapshot(paths)
            window = after[next(iter(after))]["t"] - before[next(iter(before))]["t"] if before and after else seconds
            r["container_usage"] = dockerctl.cgroup_delta(before, after)
            for v in r["container_usage"].values():
                # the workers only write during `seconds` inside a longer window (start barrier + cleanup); scale CPU to the write window
                v["cpu_cores_avg_during_inserts"] = round(v["cpu_cores_avg"] * window / seconds, 3)
                if v.get("cpu_limit_cores"):
                    v["cpu_pct_of_limit_during_inserts"] = round(100 * v["cpu_cores_avg_during_inserts"] / v["cpu_limit_cores"], 1)
            busiest = sorted(r["container_usage"].items(), key=lambda kv: -kv[1]["cpu_cores_avg_during_inserts"])[:4]
            out["runs"].append(r)
            log(f"  {tn:12s} workers={w:3d} docs/s={r['rows_per_s']:>9,} batch p50={r['batch_ms'].get('p50', 0):7.1f} ms p95={r['batch_ms'].get('p95', 0):7.1f} ms "
                f"errors={r['errors']} | busiest: " + ", ".join(f"{n.replace('nslab-', '')} {s['cpu_cores_avg_during_inserts']:.2f} cores"
                                                                + (f" ({s['cpu_pct_of_limit_during_inserts']}% of limit)" if s.get('cpu_pct_of_limit_during_inserts') is not None else "") for n, s in busiest))
            if r["first_error"]:
                log(f"    first error: {r['first_error'][:120]}")
    best = max(out["runs"], key=lambda r: r["rows_per_s"], default=None)
    if best:
        out["best"] = {"target": best["target"], "workers": best["workers"], "rows_per_s": best["rows_per_s"]}
    return out
