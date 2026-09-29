"""Phase: connection behaviour — connect latency, connection storms, concurrency scaling,
pool saturation, write contention (CAS / transaction conflicts), proxy read/write split."""
from __future__ import annotations

import queue
import random
import threading
import time
from typing import Any

from ..config import StackConfig, Target
from ..datagen import Sizes
from ..engines import Engine
from ..util import short_err, summarize


def connect_latency(engine: Engine, target: Target, n: int = 30) -> dict[str, Any]:
    samples, errors = [], []
    for _ in range(n):
        t0 = time.perf_counter_ns()
        try:
            c = engine.connect(target, timeout=10)
            c.ping()
            c.close()
            samples.append((time.perf_counter_ns() - t0) / 1e6)
        except Exception as e:  # noqa: BLE001
            errors.append(short_err(e))
    return {"target": target.name, "n": n, "ms": summarize(samples), "errors": errors[:3], "error_count": len(errors)}


def connection_storm(engine: Engine, target: Target, n: int, hold_seconds: float = 1.0) -> dict[str, Any]:
    """Open n connections concurrently, hold them all open, then close. Records connect-time
    percentiles, failures and their error codes."""
    results: list[tuple[float | None, str | None, str | None]] = [(None, None, None)] * n
    conns: list[Any] = [None] * n
    start = threading.Barrier(n + 1)
    done = threading.Event()

    def worker(i: int):
        try:
            start.wait(timeout=30)
        except threading.BrokenBarrierError:
            return
        t0 = time.perf_counter_ns()
        try:
            c = engine.connect(target, timeout=30)
            c.ping()
            conns[i] = c
            results[i] = ((time.perf_counter_ns() - t0) / 1e6, None, None)
        except Exception as e:  # noqa: BLE001
            results[i] = (None, short_err(e), engine.error_code(e))
        done.wait(timeout=hold_seconds + 60)

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(n)]
    for t in threads:
        t.start()
    t_all = time.perf_counter()
    start.wait(timeout=30)
    deadline = time.time() + 90
    while time.time() < deadline and any(r == (None, None, None) for r in results):
        time.sleep(0.05)
    total_ms = (time.perf_counter() - t_all) * 1000
    time.sleep(hold_seconds)
    done.set()
    for c in conns:
        if c is not None:
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass
    for t in threads:
        t.join(timeout=5)
    ok = [r[0] for r in results if r[0] is not None]
    errs = [r for r in results if r[1] is not None]
    codes: dict[str, int] = {}
    for _, msg, code in errs:
        k = code or (msg or "?")[:60]
        codes[k] = codes.get(k, 0) + 1
    return {"target": target.name, "requested": n, "opened": len(ok), "failed": len(errs), "wall_ms_to_open_all": round(total_ms, 1),
            "connect_ms": summarize(ok), "error_codes": codes, "sample_error": errs[0][1] if errs else None}


def concurrency_scaling(engine: Engine, target: Target, sz: Sizes, workers_list: list[int], seconds: float = 4.0) -> list[dict[str, Any]]:
    """W worker processes, each with its own connection, run point reads for `seconds`. Reports ops/s and latency."""
    from ..workers import run_workers
    out = []
    for w in workers_list:
        try:
            out.append(run_workers(engine.cfg.key, [target.name], "get_by_key", sz.customers, w, seconds))
        except Exception as e:  # noqa: BLE001
            out.append({"workers": w, "error": short_err(e)})
    return out


def pool_saturation(engine: Engine, target: Target, sz: Sizes, pool_size: int = 8, workers: int = 32, seconds: float = 4.0) -> dict[str, Any]:
    """A fixed pool of `pool_size` clients shared by `workers` threads: measures queue wait time."""
    q: queue.Queue = queue.Queue()
    for _ in range(pool_size):
        q.put(engine.connect(target, timeout=15))
    waits: list[list[float]] = [[] for _ in range(workers)]
    execs: list[list[float]] = [[] for _ in range(workers)]
    stop = time.perf_counter() + seconds

    def run(i: int):
        r = random.Random(100 + i)
        while time.perf_counter() < stop:
            t0 = time.perf_counter_ns()
            c = q.get()
            t1 = time.perf_counter_ns()
            try:
                c.op_get_by_key({"id": r.randint(1, sz.customers)})
            except Exception:  # noqa: BLE001
                pass
            finally:
                q.put(c)
            t2 = time.perf_counter_ns()
            waits[i].append((t1 - t0) / 1e6)
            execs[i].append((t2 - t1) / 1e6)

    threads = [threading.Thread(target=run, args=(i,), daemon=True) for i in range(workers)]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=seconds + 30)
    elapsed = time.perf_counter() - t0
    while not q.empty():
        try:
            q.get_nowait().close()
        except Exception:  # noqa: BLE001
            pass
    all_w = [x for l in waits for x in l]
    all_e = [x for l in execs for x in l]
    return {"pool_size": pool_size, "workers": workers, "ops": len(all_e), "ops_per_s": round(len(all_e) / elapsed) if elapsed else 0,
            "checkout_wait_ms": summarize(all_w), "exec_ms": summarize(all_e)}


def write_contention(engine: Engine, target: Target, threads: int = 4, iters: int = 50) -> dict[str, Any]:
    """`threads` clients race compare-and-set updates on the same inventory document.
    Correct CAS: applied + rejected == threads*iters and the version advanced exactly `applied` times."""
    key = {"product_id": 1, "warehouse_id": 1}
    admin = engine.connect(target)
    try:
        if not hasattr(admin, "op_conditional_update"):
            return {"status": "n/a", "note": "no compare-and-set operation for this engine"}
        try:
            before = admin.op_conditional_update(key)[0][1]
        except Exception as e:  # noqa: BLE001
            return {"status": "n/a", "error": short_err(e)}
        applied = [0] * threads
        rejected = [0] * threads
        errors = [0] * threads
        first_err: list[str | None] = [None]
        lat: list[list[float]] = [[] for _ in range(threads)]
        clients = [engine.connect(target) for _ in range(threads)]
        barrier = threading.Barrier(threads)

        def run(i: int):
            c = clients[i]
            barrier.wait()
            for _ in range(iters):
                t0 = time.perf_counter_ns()
                try:
                    r = c.op_conditional_update(key)
                    if r and r[0][0]:
                        applied[i] += 1
                    else:
                        rejected[i] += 1
                except Exception as e:  # noqa: BLE001
                    errors[i] += 1
                    first_err[0] = first_err[0] or short_err(e)
                lat[i].append((time.perf_counter_ns() - t0) / 1e6)

        ths = [threading.Thread(target=run, args=(i,), daemon=True) for i in range(threads)]
        t0 = time.perf_counter()
        for t in ths:
            t.start()
        for t in ths:
            t.join(timeout=120)
        secs = time.perf_counter() - t0
        for c in clients:
            c.close()
        after = admin.op_conditional_update(key)[0][1] - 1
        a, rj = sum(applied), sum(rejected)
        return {"threads": threads, "iters_per_thread": iters, "applied": a, "rejected": rj, "errors": sum(errors),
                "version_before": before, "version_after": after, "version_delta": after - before,
                "atomic": (after - before) == a, "seconds": round(secs, 2), "cas_ms": summarize([x for l in lat for x in l]),
                "first_error": first_err[0],
                "interpretation": ("every accepted CAS advanced the version exactly once: atomic compare-and-set"
                                   if (after - before) == a else "version drifted from applied count: lost or duplicated updates")}
    finally:
        admin.close()


def rw_split(engine: Engine, proxy: Target, n: int = 40) -> dict[str, Any]:
    """Through a load-balancing proxy: which backend answers, and do writes succeed?"""
    reads: dict[str, int] = {}
    writes_ok: dict[str, int] = {}
    writes_rejected: dict[str, int] = {}
    codes: dict[str, int] = {}
    for i in range(n):
        try:
            c = engine.connect(proxy, timeout=10)
        except Exception as e:  # noqa: BLE001
            k = f"connect: {short_err(e)[:60]}"
            codes[k] = codes.get(k, 0) + 1
            continue
        try:
            key = c.role()
            reads[key] = reads.get(key, 0) + 1
            try:
                c.put("repl_probe", {"id": 900_000 + i, "ts_ms": int(time.time() * 1000)})
                writes_ok[key] = writes_ok.get(key, 0) + 1
            except Exception as e:  # noqa: BLE001
                writes_rejected[key] = writes_rejected.get(key, 0) + 1
                code = engine.error_code(e) or short_err(e)[:60]
                codes[code] = codes.get(code, 0) + 1
        finally:
            c.close()
    n_ok = sum(writes_ok.values())
    n_rej = sum(writes_rejected.values())
    if n_rej == 0 and n_ok and len(reads) > 1:
        interp = "connections spread over several backends and every write succeeded"
    elif n_rej == 0 and n_ok:
        interp = "all traffic served by one writable backend"
    elif n_ok and n_rej:
        interp = f"TCP-level balancing: {n_rej}/{n} writes failed because the connection landed on a read-only replica"
    else:
        interp = "no successful writes through the proxy"
    return {"proxy": proxy.name, "connections": n, "roles_seen": reads, "writes_ok": n_ok, "writes_rejected": n_rej,
            "writes_rejected_on": writes_rejected, "write_error_codes": codes, "interpretation": interp}


def run_connections(engine: Engine, cfg: StackConfig, scale: float, *, log=print, storms: list[int] | None = None) -> dict[str, Any]:
    sz = Sizes.for_scale(scale)
    out: dict[str, Any] = {}
    primary = cfg.primary
    c = engine.connect_primary()
    try:
        out["max_connections"] = engine.max_connections(c)
    finally:
        c.close()
    log(f"  max_connections={out['max_connections']}")
    targets = [primary] + cfg.proxies
    out["connect_latency"] = {}
    for t in targets:
        r = connect_latency(engine, t)
        out["connect_latency"][t.name] = r
        log(f"  connect latency via {t.name:10s} p50={r['ms'].get('p50', 0):.2f} ms p95={r['ms'].get('p95', 0):.2f} ms errors={r['error_count']}")
    if cfg.features.get("connection_storm", True) and not engine.embedded:
        storm_sizes = storms or [10, 50, 100, 200, 500]
        mx = out["max_connections"]
        if mx and 0 < mx < 5000:
            storm_sizes = [s for s in storm_sizes if s < mx] + [mx + 10]
        out["storm"] = {}
        for t in targets:
            out["storm"][t.name] = []
            for n in storm_sizes:
                if t.role == "proxy" and n > 500:
                    continue
                r = connection_storm(engine, t, n)
                out["storm"][t.name].append(r)
                log(f"  storm via {t.name:10s} n={n:4d} opened={r['opened']:4d} failed={r['failed']:3d} wall={r['wall_ms_to_open_all']:8.1f} ms "
                    f"connect p95={r['connect_ms'].get('p95', 0):.1f} ms codes={r['error_codes']}")
    out["concurrency"] = {}
    for t in targets:
        r = concurrency_scaling(engine, t, sz, cfg.features.get("concurrency_workers") or [1, 4, 16, 32, 64])
        out["concurrency"][t.name] = r
        for x in r:
            if "ops_per_s" in x:
                log(f"  workers={x['workers']:3d} via {t.name:10s} ops/s={x['ops_per_s']:7d} p50={x['latency_ms'].get('p50', 0):.3f} ms p99={x['latency_ms'].get('p99', 0):.3f} ms errors={x['errors']} {x.get('first_error') or ''}")
            else:
                log(f"  workers={x['workers']:3d} via {t.name:10s} ERR {x['error'][:80]}")
    try:
        r = pool_saturation(engine, primary, sz)
        out["pool_saturation"] = r
        log(f"  pool 8 clients / 32 threads: ops/s={r['ops_per_s']} checkout wait p50={r['checkout_wait_ms'].get('p50', 0):.3f} ms p99={r['checkout_wait_ms'].get('p99', 0):.3f} ms")
    except Exception as e:  # noqa: BLE001
        out["pool_saturation"] = {"error": short_err(e)}
    try:
        out["write_contention"] = write_contention(engine, primary)
        log(f"  write contention: {str({k: v for k, v in out['write_contention'].items() if k not in ('cas_ms',)})[:220]}")
    except Exception as e:  # noqa: BLE001
        out["write_contention"] = {"error": short_err(e)}
    for p in cfg.proxies:
        if p.extra.get("splits_reads"):
            out.setdefault("rw_split", {})[p.name] = rw_split(engine, p)
            log(f"  rw split via {p.name}: {out['rw_split'][p.name]}")
    return out
