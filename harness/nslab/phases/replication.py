"""Phase: replication / scaling — topology, visibility lag, write rejection, lag under load,
read scaling across replicas, and (optionally) failover."""
from __future__ import annotations

import threading
import time
from typing import Any

from .. import dockerctl
from ..config import StackConfig, Target
from ..datagen import Sizes
from ..engines import Engine
from ..engines.base import with_timeout
from ..util import short_err, summarize

PROBE = "repl_probe"


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _read_targets(cfg: StackConfig, primary: Target) -> list[Target]:
    rep = cfg.replication or {}
    if rep.get("read_targets"):
        return [t for t in (cfg.by_name(n) for n in rep["read_targets"]) if t is not None]
    reps = cfg.replicas
    if reps:
        return reps
    return [n for n in cfg.nodes if n is not primary]


def visibility_lag(engine: Engine, primary: Target, replica: Target, n: int = 30, timeout: float = 30.0, base_id: int = 1000) -> dict[str, Any]:
    p = engine.connect(primary)
    r = engine.connect(replica)
    lags, misses = [], 0
    try:
        for i in range(n):
            p.put(PROBE, {"id": base_id + i, "ts_ms": _now_ms()})
            t_commit = time.perf_counter()
            deadline = t_commit + timeout
            seen = False
            while time.perf_counter() < deadline:
                if r.get(PROBE, base_id + i):
                    lags.append((time.perf_counter() - t_commit) * 1000)
                    seen = True
                    break
                time.sleep(0.001)
            if not seen:
                misses += 1
            time.sleep(0.05)
    finally:
        p.close()
        r.close()
    return {"replica": replica.name, "n": n, "visible": n - misses, "timeouts": misses, "lag_ms": summarize(lags)}


def write_rejection(engine: Engine, replica: Target) -> dict[str, Any]:
    c = engine.connect(replica)
    try:
        try:
            c.put(PROBE, {"id": 999999, "ts_ms": _now_ms()})
            try:
                c.delete(PROBE, 999999)
            except Exception:  # noqa: BLE001
                pass
            return {"replica": replica.name, "writes_rejected": False, "note": "replica accepted a write (multi-master / leaderless / coordinator forwards)"}
        except Exception as e:  # noqa: BLE001
            return {"replica": replica.name, "writes_rejected": True, "error_code": engine.error_code(e), "error": short_err(e)}
    finally:
        c.close()


def lag_under_load(engine: Engine, cfg: StackConfig, primary: Target, replicas: list[Target], rows: int = 20000, batch: int = 500,
                   sample_every: float = 0.25) -> dict[str, Any]:
    p = engine.connect(primary)
    ps = engine.connect(primary)
    stop = threading.Event()
    samples: list[dict[str, Any]] = []
    rconns = [engine.connect(r) for r in replicas]

    def sampler():
        while not stop.is_set():
            snap = {"t": round(time.perf_counter(), 3)}
            try:
                snap["primary"] = engine.replication_status(ps)
            except Exception as e:  # noqa: BLE001
                snap["primary"] = short_err(e)
            for r, rc in zip(replicas, rconns):
                try:
                    snap[r.name] = engine.replication_status(rc)
                except Exception as e:  # noqa: BLE001
                    snap[r.name] = short_err(e)
            samples.append(snap)
            time.sleep(sample_every)

    th = threading.Thread(target=sampler, daemon=True)
    th.start()
    t0 = time.perf_counter()
    base = 5_000_000
    try:
        for i in range(0, rows, batch):
            p.bulk_put(PROBE, [{"id": base + i + j, "ts_ms": _now_ms()} for j in range(min(batch, rows - i))])
    except Exception as e:  # noqa: BLE001
        stop.set()
        ps.close()
        return {"error": short_err(e)}
    write_secs = time.perf_counter() - t0
    expected = p.count(PROBE)
    catchup = {}
    for r, rc in zip(replicas, rconns):
        t1 = time.perf_counter()
        deadline = t1 + 120
        while time.perf_counter() < deadline:
            try:
                if rc.count(PROBE) >= expected:
                    break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.02)
        catchup[r.name] = round((time.perf_counter() - t1) * 1000, 1)
    stop.set()
    th.join(timeout=5)
    for rc in rconns:
        rc.close()
    p.close()
    ps.close()
    return {"rows": rows, "batch": batch, "write_seconds": round(write_secs, 2), "rows_per_s": round(rows / write_secs) if write_secs else None,
            "catchup_ms_after_last_write": catchup, "samples": samples[:120]}


def read_scaling(engine: Engine, targets: list[Target], sz: Sizes, workers: int = 32, seconds: float = 4.0) -> dict[str, Any]:
    from ..workers import run_workers
    r = run_workers(engine.cfg.key, [t.name for t in targets], "get_by_key", sz.customers, workers, seconds)
    return {"targets": [t.name for t in targets], "workers": workers, "ops": r["ops"], "ops_per_s": r["ops_per_s"], "errors": r["errors"], "latency_ms": r["latency_ms"]}


def failover(engine: Engine, cfg: StackConfig, *, log=print) -> dict[str, Any]:
    """Kill the primary container, promote (or let the cluster elect), measure time until writes succeed again."""
    rep = cfg.replication or {}
    primary = cfg.by_name(rep.get("write_target")) or cfg.primary
    verify = cfg.by_name(rep.get("verify_target"))
    kind = rep.get("kind", "none")
    # elected clusters (Raft leader, ES master): kill the member that currently holds the role, not a fixed container
    leader_fn = getattr(engine, "leader_target", None)
    if leader_fn:
        try:
            lt = leader_fn()
            if lt is not None and lt.container:
                primary = lt
        except Exception as e:  # noqa: BLE001
            log(f"  leader lookup failed ({short_err(e)}); killing the configured primary")
    out: dict[str, Any] = {"kind": kind, "killed": primary.name, "killed_container": primary.container}
    if not primary.container:
        return {"status": "n/a", "note": "primary has no container to stop"}
    candidates = cfg.replicas or [t for t in cfg.nodes if t is not primary and t.container != primary.container]
    if not candidates:
        return {"status": "n/a", "note": "no replica / secondary node configured"}
    new_primary = cfg.by_name(rep.get("promote", {}).get("target")) or candidates[0]
    if new_primary.container == primary.container:
        new_primary = candidates[0]
    if verify is not None and verify.container == primary.container:       # the verify entry point would die with the leader
        alt = [t for t in cfg.targets if t.container and t.container != primary.container and t.extra.get("routing")] or \
              [t for t in cfg.targets if t.container and t.container != primary.container and t.role in ("node", "replica")]
        verify = alt[0] if alt else None
    poll_target = verify or new_primary

    def _probe_once(base):
        c = engine.connect(poll_target, timeout=3)
        try:
            c.put(PROBE, {"id": base, "ts_ms": _now_ms()})
        finally:
            c.close()

    log(f"  killing primary container {primary.container} ...")
    t_stop = time.perf_counter()
    dockerctl.container_action(primary.container, "kill")
    try:
        with_timeout(lambda: _probe_once(7_000_000), 20, "post-kill write probe")
        out["writes_still_succeed_after_kill"] = True
    except Exception as e:  # noqa: BLE001
        out["write_error_after_kill"] = short_err(e)
    promote_method = rep.get("promote", {}).get("method", "engine")
    if promote_method not in ("none", "auto"):
        try:
            t_p = time.perf_counter()
            out["promote_cmd"] = with_timeout(lambda: engine.promote(new_primary), 90, "promote")
            out["promote_ms"] = round((time.perf_counter() - t_p) * 1000, 1)
            log(f"  promoted {new_primary.name} via: {out['promote_cmd']}")
        except Exception as e:  # noqa: BLE001
            out["promote_error"] = short_err(e)
            log(f"  promote failed: {short_err(e)}")
    deadline = time.perf_counter() + 180
    attempts, first_ok, last_err = 0, None, None
    while time.perf_counter() < deadline:
        attempts += 1
        try:
            with_timeout(lambda: _probe_once(7_100_000 + attempts), 15, "write probe")
            first_ok = time.perf_counter()
            break
        except Exception as e:  # noqa: BLE001
            last_err = short_err(e)
            time.sleep(0.2)
    out["write_attempts_until_success"] = attempts
    out["last_error_before_success"] = last_err
    out["downtime_ms"] = round((first_ok - t_stop) * 1000, 1) if first_ok else None
    if leader_fn and first_ok:
        try:
            lt = leader_fn()
            if lt is not None:
                new_primary = lt
        except Exception:  # noqa: BLE001
            pass
    out["new_primary"] = new_primary.name
    out["verified_via"] = poll_target.name
    try:
        c = engine.connect(new_primary)
        out["new_primary_role"] = c.role()
        out["new_primary_status"] = engine.replication_status(c)
        c.close()
    except Exception as e:  # noqa: BLE001
        out["new_primary_status_error"] = short_err(e)
    log(f"  downtime until first successful write via {poll_target.name}: {out['downtime_ms']} ms ({attempts} attempts)")
    if rep.get("restart_old_primary", True):
        try:
            dockerctl.container_action(primary.container, "start")
            out["old_primary_restarted"] = True
            out["note"] = "old primary restarted; whether it rejoins depends on the engine (stack should be recreated for reuse)"
        except Exception as e:  # noqa: BLE001
            out["old_primary_restart_error"] = short_err(e)
    return out


def run_replication(engine: Engine, cfg: StackConfig, scale: float, *, do_failover: bool = False, log=print) -> dict[str, Any]:
    sz = Sizes.for_scale(scale)
    rep = cfg.replication or {}
    out: dict[str, Any] = {"kind": rep.get("kind", "none")}
    primary = cfg.by_name(rep.get("write_target")) or cfg.primary
    read_targets = _read_targets(cfg, primary)
    topo = {}
    for t in cfg.targets:
        if t.role in ("sentinel",):
            continue
        try:
            c = engine.connect(t, timeout=10)
            topo[t.name] = {"role_reported": c.role(), "role_configured": t.role, "version": c.server_version()[:120], "status": engine.replication_status(c)}
            c.close()
        except Exception as e:  # noqa: BLE001
            topo[t.name] = {"error": short_err(e)}
        log(f"  {t.name:10s} role={topo[t.name].get('role_reported')} {str(topo[t.name].get('status', topo[t.name].get('error', '')))[:120]}")
    out["topology"] = topo
    if not read_targets:
        out["status"] = "single-node"
        log("  single node stack: no replication tests")
        return out
    p = engine.connect(primary)
    try:
        try:
            p.drop_collection(PROBE) if not getattr(engine, "shared_keyspace", False) else None
        except Exception:  # noqa: BLE001
            pass
        if hasattr(engine, "create_probe_collection"):
            engine.create_probe_collection(p)
    finally:
        p.close()
    for r in read_targets:
        try:
            c = engine.connect(r)
            dockerctl.wait_for(lambda: c.count(PROBE) is not None, timeout=60, desc="probe collection on replica")
            c.close()
        except Exception as e:  # noqa: BLE001
            log(f"  probe collection not visible on {r.name}: {short_err(e)}")
    out["visibility_lag"] = {}
    for ri, r in enumerate(read_targets):
        try:
            v = visibility_lag(engine, primary, r, base_id=1000 + 1000 * ri)
            out["visibility_lag"][r.name] = v
            log(f"  visibility lag -> {r.name}: p50={v['lag_ms'].get('p50', 0):.2f} ms p95={v['lag_ms'].get('p95', 0):.2f} ms max={v['lag_ms'].get('max', 0):.2f} ms timeouts={v['timeouts']}")
        except Exception as e:  # noqa: BLE001
            out["visibility_lag"][r.name] = {"error": short_err(e)}
            log(f"  visibility lag -> {r.name}: ERR {short_err(e)}")
    out["write_rejection"] = {}
    for r in read_targets:
        w = write_rejection(engine, r)
        out["write_rejection"][r.name] = w
        log(f"  write on {r.name}: {'rejected ' + str(w.get('error_code') or w.get('error', '')[:60]) if w['writes_rejected'] else 'ACCEPTED'}")
    try:
        l = lag_under_load(engine, cfg, primary, read_targets)
        out["lag_under_load"] = l
        log(f"  lag under load: {l.get('rows')} docs in {l.get('write_seconds')}s, catch-up after last write: {l.get('catchup_ms_after_last_write')}")
    except Exception as e:  # noqa: BLE001
        out["lag_under_load"] = {"error": short_err(e)}
    try:
        base_t = cfg.by_name(rep.get("read_scaling_base")) or primary
        rs_targets = [cfg.by_name(n) for n in rep.get("read_scaling_targets", [])] or ([base_t] + read_targets)
        a = read_scaling(engine, [base_t], sz)
        b = read_scaling(engine, [t for t in rs_targets if t is not None], sz)
        out["read_scaling"] = {"primary_only": a, "primary_plus_replicas": b, "gain": round(b["ops_per_s"] / a["ops_per_s"], 2) if a["ops_per_s"] else None}
        log(f"  read scaling: primary only {a['ops_per_s']} ops/s -> primary+replicas {b['ops_per_s']} ops/s (x{out['read_scaling']['gain']}) errors={b['errors']}")
    except Exception as e:  # noqa: BLE001
        out["read_scaling"] = {"error": short_err(e)}
    if do_failover:
        try:
            out["failover"] = failover(engine, cfg, log=log)
        except Exception as e:  # noqa: BLE001
            out["failover"] = {"error": short_err(e)}
    return out
