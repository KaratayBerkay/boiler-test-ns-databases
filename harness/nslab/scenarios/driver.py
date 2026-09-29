"""Generic scenario load runner: a weighted, hotspot-skewed, multi-process steady-state ramp.

Scenario-agnostic — it drives whatever `Scenario` (base.py) it is handed: builds the corpus + backend + op mix in
each forked worker, runs the weighted mix (closed-loop with optional think-time, or open-loop at a target rate) for
each worker count, and records per-operation latency percentiles, throughput, errors and per-container cgroup usage.
Seeding and analysis curves are scenario-specific (defined on each Scenario).
"""
from __future__ import annotations

import multiprocessing as mp
import random
import time
from typing import Any

from .. import dockerctl
from ..config import StackConfig, load_stack
from ..util import summarize
from . import base


def _run_worker(scen_name: str, stack_key: str, size: str, w: int, seconds: float, mix_names: list[str], opts: dict,
                start_at: float, rate: float | None, think_ms: float, q: mp.Queue) -> None:
    scen = base.get(scen_name)
    cfg = load_stack(stack_key)
    corpus = scen.make_corpus(scen.sizes_named(size))
    be = scen.make_backend(stack_key, cfg, opts)
    be.connect()
    mbn = {o.name: o for o in scen.mix()}
    ops = [mbn[n] for n in mix_names]
    weights = [o.weight for o in ops]
    rng = random.Random(1000 + w)
    ctx = base.Ctx(backend=be, corpus=corpus, rng=rng)
    lat: dict[str, list[float]] = {o.name: [] for o in ops}
    errs: dict[str, int] = {o.name: 0 for o in ops}
    first_err: dict[str, str] = {}
    interval = (1.0 / rate) if rate else 0.0
    while time.time() < start_at:
        time.sleep(0.001)
    next_t = time.perf_counter()
    stop = time.perf_counter() + seconds
    while time.perf_counter() < stop:
        op = rng.choices(ops, weights=weights, k=1)[0]
        t0 = time.perf_counter_ns()
        try:
            op.run(ctx)
            lat[op.name].append((time.perf_counter_ns() - t0) / 1e6)
        except Exception as e:  # noqa: BLE001
            errs[op.name] += 1
            first_err.setdefault(op.name, f"{type(e).__name__}: {e}"[:200])
        if think_ms:
            time.sleep(think_ms / 1000.0)
        if interval:
            next_t += interval
            slp = next_t - time.perf_counter()
            if slp > 0:
                time.sleep(slp)
    be.close()
    lat = {k: (v if len(v) <= 40000 else random.sample(v, 40000)) for k, v in lat.items()}
    q.put({"w": w, "lat": lat, "errs": errs, "first_err": first_err})


def _run_at_workers(scen_name: str, stack_key: str, size: str, workers: int, seconds: float, mix_names: list[str],
                    opts: dict, rate: float | None, think_ms: float, containers: list[str], log) -> dict[str, Any]:
    ctx = mp.get_context("fork")
    q: mp.Queue = ctx.Queue()
    start_at = time.time() + min(5.0, 0.05 * workers + 1.5)
    per_worker_rate = (rate / workers) if rate else None
    procs = [ctx.Process(target=_run_worker, args=(scen_name, stack_key, size, w, seconds, mix_names, opts, start_at,
                                                    per_worker_rate, think_ms, q), daemon=True) for w in range(workers)]
    paths = dockerctl.cgroup_paths(containers)
    before = dockerctl.cgroup_snapshot(paths)
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
    after = dockerctl.cgroup_snapshot(paths)
    window = (after[next(iter(after))]["t"] - before[next(iter(before))]["t"]) if before and after else seconds
    per_op: dict[str, dict] = {}
    total_ops = total_err = 0
    for name in mix_names:
        samples = [x for r in results for x in r["lat"].get(name, [])]
        e = sum(r["errs"].get(name, 0) for r in results)
        fe = next((r["first_err"].get(name) for r in results if r["first_err"].get(name)), None)
        total_ops += len(samples)
        total_err += e
        per_op[name] = {"n": len(samples), "errors": e, "first_error": fe, "latency_ms": summarize(samples)}
    usage = dockerctl.cgroup_delta(before, after)
    for u in usage.values():
        u["cpu_cores_avg_during_run"] = round(u["cpu_cores_avg"] * window / seconds, 3)
    run = {"workers": workers, "seconds": seconds, "mode": "open" if rate else "closed", "rate": rate, "think_ms": think_ms,
           "throughput_ops_s": round(total_ops / seconds) if seconds else 0, "ops": total_ops, "errors": total_err,
           "per_op": per_op, "container_usage": usage}
    busiest = sorted(usage.items(), key=lambda kv: -kv[1]["cpu_cores_avg_during_run"])[:3]
    top = sorted(mix_names, key=lambda n: -next((o.weight for o in base.get(scen_name).mix() if o.name == n), 0))[:4]
    log(f"  workers={workers:3d} throughput={run['throughput_ops_s']:>7,} ops/s errors={total_err} | "
        + "  ".join(f"{n} p95={per_op[n]['latency_ms'].get('p95', 0):.1f}" for n in top)
        + " ms | busiest " + ", ".join(f"{n.replace('nslab-','')} {u['cpu_cores_avg_during_run']:.1f}c" for n, u in busiest))
    return run


def run(scen_name: str, stack_key: str, size: str, *, workers_list: list[int], seconds: float, opts: dict,
        mix_names: list[str] | None, rate: float | None, think_ms: float, log=print) -> dict[str, Any]:
    scen = base.get(scen_name)
    cfg = load_stack(stack_key)
    mix_names = mix_names or [o.name for o in scen.mix()]
    containers = cfg.containers
    be = scen.make_backend(stack_key, cfg, opts); be.connect()
    server = be.server_info()
    be.close()
    log(f"  {server}")
    out = {"scenario": scen_name, "engine": stack_key, "size": size, "opts": opts,
           "mix": {o.name: o.weight for o in scen.mix() if o.name in mix_names}, "server": server, "runs": []}
    for wc in workers_list:
        out["runs"].append(_run_at_workers(scen_name, stack_key, size, wc, seconds, mix_names, opts, rate, think_ms, containers, log))
    best = max(out["runs"], key=lambda r: r["throughput_ops_s"], default=None)
    if best:
        out["peak_throughput_ops_s"] = best["throughput_ops_s"]
        out["peak_workers"] = best["workers"]
    return out
