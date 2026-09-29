"""Phase: logging — is the engine's log strategy actually usable? For every stack the phase records

  1. the container log driver/rotation (docker inspect), how much the engine has logged so far and whether the
     lines are JSON (a shipper-friendly structured log) or free text;
  2. the engine's own log location/format (server_log_facts);
  3. the slow-query log: set a threshold, run one point read (must not appear) and one known-slow operation
     (must appear), read the entries back through the engine's own interface (SLOWLOG GET, system.profile /
     getLog, debug.log, query.log, index slowlog) and check that the slow operation is there with its duration;
  4. the audit / full-query log: enable, run a few statements, read back, disable;
  5. changing the log verbosity at run time (no restart);
  6. how many log bytes/lines the engine produced while all of that ran (log volume per phase).
"""
from __future__ import annotations

import time
from typing import Any

from .. import dockerctl
from ..config import StackConfig
from ..engines import Engine
from ..ops import Unsupported
from ..util import short_err

SLOW_THRESHOLD_MS = 100


def _try(fn, *a, **kw) -> dict[str, Any]:
    t0 = time.perf_counter()
    try:
        v = fn(*a, **kw)
        return {"status": "ok", "value": v, "ms": round((time.perf_counter() - t0) * 1000, 1)}
    except Unsupported as e:
        return {"status": "unsupported", "detail": str(e)[:300], "ms": round((time.perf_counter() - t0) * 1000, 1)}
    except Exception as e:  # noqa: BLE001
        return {"status": "error", "error": short_err(e), "ms": round((time.perf_counter() - t0) * 1000, 1)}


def run_logging(engine: Engine, cfg: StackConfig, scale: float, *, log=print) -> dict[str, Any]:
    out: dict[str, Any] = {"strategy": dict(engine.LOGGING_STRATEGY), "threshold_ms": SLOW_THRESHOLD_MS}
    # 1. container-level facts
    facts: dict[str, Any] = {}
    for c in cfg.containers:
        facts[c] = dockerctl.log_facts(c)
        f = facts[c]
        if "error" not in f:
            log(f"  {c.replace('nslab-', ''):18s} driver={f['driver']} rotation={f['options']} logged so far {f['bytes']:,} B / {f['lines']} lines  format={f['format']}")
    out["containers"] = facts
    before = {c: (f.get("bytes", 0), f.get("lines", 0)) for c, f in facts.items() if "error" not in f}
    client = engine.connect_primary(timeout=30)
    try:
        # 2. the engine's own log
        out["server_log"] = _try(engine.server_log_facts, client)
        if out["server_log"]["status"] == "ok":
            v = out["server_log"]["value"] or {}
            log(f"  server log: {v.get('format', '?')} at {v.get('where', '?')}")
        # 3. slow-query log
        sq: dict[str, Any] = {}
        st = _try(engine.slow_query_setup, client, SLOW_THRESHOLD_MS)
        threshold = SLOW_THRESHOLD_MS
        if st["status"] == "ok" and isinstance(st["value"], tuple):        # adapters that cannot change the threshold report the effective one
            st["value"], threshold = st["value"][0], int(st["value"][1])
        sq["setup"] = st
        sq["effective_threshold_ms"] = threshold
        if st["status"] == "ok":
            log(f"  slow-query log: {st['value']}")
            fast = _try(engine.slow_query_fast_op, client)
            slow = _try(engine.slow_query_slow_op, client)
            sq["fast_op"] = {"desc": fast.get("value"), "ms": fast["ms"], "status": fast["status"], "error": fast.get("error")}
            sq["slow_op"] = {"desc": slow.get("value"), "ms": slow["ms"], "status": slow["status"], "error": slow.get("error")}
            time.sleep(float(engine.features.get("slowlog_settle_seconds", 0.5)))
            ent = _try(engine.slow_query_entries, client, threshold)
            entries = ent.get("value") or []
            sq["entries"] = entries[:10]
            sq["entries_status"] = ent["status"]
            sq["entries_error"] = ent.get("error") or ent.get("detail")
            slow_desc = (slow.get("value") or "").lower()
            sig = engine.features.get("slowlog_signature") or (slow_desc.split(" ")[0] if slow_desc else "")
            hit = next((e for e in entries if sig and sig in str(e.get("text", "")).lower()), None)
            fast_desc = (fast.get("value") or "").lower()
            fast_sig = fast_desc.split(" ")[0] if fast_desc else ""
            fast_hit = any(fast_sig and fast_sig in str(e.get("text", "")).lower() and float(e.get("ms") or 0) < threshold for e in entries)
            sq["slow_op_logged"] = hit is not None
            sq["logged_ms"] = hit.get("ms") if hit else None
            sq["fast_op_logged"] = fast_hit
            sq["verified"] = bool(hit) and not fast_hit
            log(f"  slow op '{(slow.get('value') or '')[:60]}' took {slow['ms']} ms -> logged={sq['slow_op_logged']} ({sq['logged_ms']} ms in log); "
                f"fast op {fast['ms']} ms -> logged={fast_hit}; entries read back: {len(entries)}")
            try:
                engine.slow_query_teardown(client)
            except Exception:  # noqa: BLE001
                pass
        else:
            log(f"  slow-query log: {st['status']} {st.get('detail') or st.get('error')}")
        out["slow_query"] = sq
        # 4. audit log
        au: dict[str, Any] = {}
        st = _try(engine.audit_log_setup, client)
        au["setup"] = st
        if st["status"] == "ok":
            ex = _try(engine.audit_log_exercise, client)
            au["exercise"] = ex.get("value") or []
            time.sleep(float(engine.features.get("audit_settle_seconds", 1.0)))
            ent = _try(engine.audit_log_entries, client)
            entries = ent.get("value") or []
            au["entries"] = [str(e)[:300] for e in entries[:10]]
            au["entries_count"] = len(entries)
            au["entries_status"] = ent["status"]
            au["entries_error"] = ent.get("error") or ent.get("detail")
            au["verified"] = ent["status"] == "ok" and len(entries) > 0
            log(f"  audit log: {st['value']} -> {len(entries)} entries read back (verified={au['verified']})")
            try:
                engine.audit_log_teardown(client)
            except Exception:  # noqa: BLE001
                pass
        else:
            log(f"  audit log: {st['status']} {st.get('detail') or st.get('error')}")
        out["audit"] = au
        # 5. runtime log level
        rl = _try(engine.log_level_runtime, client)
        out["runtime_log_level"] = rl
        log(f"  runtime log level: {rl['status']} {rl.get('value') or rl.get('detail') or rl.get('error')}")
    finally:
        client.close()
    # 6. log volume produced by this phase
    vol: dict[str, Any] = {}
    for c, (b0, l0) in before.items():
        f = dockerctl.log_facts(c, sample_lines=50)
        if "error" in f:
            continue
        vol[c] = {"bytes": f["bytes"] - b0, "lines": f["lines"] - l0}
    out["log_volume_this_phase"] = vol
    if vol:
        log("  log volume during this phase: " + ", ".join(f"{c.replace('nslab-', '')} {v['bytes']:,} B/{v['lines']} lines" for c, v in vol.items()))
    return out
