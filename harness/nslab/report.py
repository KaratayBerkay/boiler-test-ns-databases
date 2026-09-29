"""Persist per-engine results and build the cross-engine summary (results/SUMMARY.md + summary.json)."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from .engines.base import looks_unsupported
from .ops import CATALOG
from .probes import PROBES
from .util import RESULTS, dump_json, load_json, now_iso, results_root


def _status(rec: dict) -> str:
    st = rec.get("status")
    if st == "error" and looks_unsupported(rec.get("error")):
        return "unsupported"
    return st


def save_result(key: str, run_id: str, result: dict[str, Any]) -> Path:
    from . import platform
    result.setdefault("platform", "k3s" if platform.k8s() else "compose")
    d = results_root() / key
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{run_id}.json"
    dump_json(p, result)
    latest = d / "latest.json"
    merged = load_json(latest) if latest.exists() else {}
    merged.update({k: v for k, v in result.items() if k not in ("phases", "partial")})
    phases = merged.setdefault("phases", {})
    for k, v in result.get("phases", {}).items():
        if result.get("partial") and isinstance(v, dict) and isinstance(phases.get(k), dict):
            phases[k].update(v)          # `--only` run: merge into the existing phase instead of replacing it
        else:
            phases[k] = v
    dump_json(latest, merged)
    return p


def _fmt_ms(v) -> str:
    if v is None:
        return "-"
    if v >= 1000:
        return f"{v/1000:.2f} s"
    if v >= 10:
        return f"{v:.0f}"
    return f"{v:.2f}"


def _fmt_bytes(v) -> str:
    if v is None or not isinstance(v, (int, float)):
        return "-"
    if v >= 1e9:
        return f"{v/1e9:.2f} GB"
    if v >= 1e6:
        return f"{v/1e6:.1f} MB"
    if v >= 1e3:
        return f"{v/1e3:.0f} KB"
    return f"{v} B"


def load_all() -> dict[str, dict[str, Any]]:
    """Every engine's latest.json; k3s runs (results/k3s/<engine>/) come in as `<engine>@k3s` with a "(k3s)" display."""
    out = {}
    if not RESULTS.exists():
        return out
    for d in sorted(RESULTS.iterdir()):
        f = d / "latest.json"
        if f.exists():
            out[d.name] = load_json(f)
    k3s = RESULTS / "k3s"
    if k3s.exists():
        for d in sorted(k3s.iterdir()):
            f = d / "latest.json"
            if f.exists():
                r = load_json(f)
                r["display"] = f"{r.get('display', d.name)} (k3s)"
                r["platform"] = "k3s"
                out[f"{d.name}@k3s"] = r
    return out


def build_summary() -> tuple[Path, Path]:
    all_res = load_all()
    lines: list[str] = []
    keys = list(all_res)
    disp = {k: all_res[k].get("display", k) for k in keys}
    lines.append("# ns-databases — cross-engine results\n")
    lines.append(f"Generated {now_iso()} from `results/<engine>/latest.json`. Timings are client-observed p50 latencies in milliseconds "
                 "(warm cache, single client, 10 iterations unless noted). `--` = the engine has no way to express the operation, `ERR` = failed at run time.\n")
    # --- engine table
    lines.append("## Engines\n")
    lines.append("| engine | version | family | category | image | load docs/s (events) | schema+load s | replication |")
    lines.append("|---|---|---|---|---|---:|---:|---|")
    for k in keys:
        r = all_res[k]
        ld = r.get("phases", {}).get("load", {})
        ev = ld.get("collections", {}).get("events", {})
        rep = r.get("phases", {}).get("replication", {})
        lines.append(f"| {disp[k]} | {str(ld.get('server_version', r.get('server_version', '')))[:60]} | {r.get('family', '')} | {r.get('category', '')} | `{r.get('image', '')}` | "
                     f"{ev.get('rows_per_s') or '-'} | {ld.get('load_seconds', '-')} | {rep.get('kind', '-')} |")
    # --- operation matrix
    lines.append("\n## Operation capability & latency matrix (p50 ms)\n")
    lines.append("| operation | " + " | ".join(disp[k] for k in keys) + " |")
    lines.append("|---|" + "---:|" * len(keys))
    for op in CATALOG:
        row = [f"`{op.id}`"]
        for k in keys:
            b = all_res[k].get("phases", {}).get("bench", {}).get(op.id)
            if not b:
                row.append("·")
            elif b.get("status") == "ok":
                row.append(_fmt_ms(b["timing"].get("p50")))
            elif _status(b) == "unsupported":
                row.append("--")
            else:
                row.append("ERR")
        lines.append("| " + " | ".join(row) + " |")
    # --- capability probes
    lines.append("\n## Feature probes\n")
    lines.append("| feature | " + " | ".join(disp[k] for k in keys) + " |")
    lines.append("|---|" + ":---:|" * len(keys))
    sym = {"supported": "✅", "unsupported": "❌", "error": "⚠️"}
    for p in PROBES:
        row = [p.name]
        for k in keys:
            c = all_res[k].get("phases", {}).get("capabilities", {}).get(p.name)
            row.append(sym.get(_status(c), "?") if isinstance(c, dict) else "·")
        lines.append("| " + " | ".join(row) + " |")
    # --- optimisation
    lines.append("\n## Optimisation experiments (speed-up of best variant vs first variant, p50)\n")
    exp_ids: list[str] = []
    for k in keys:
        for e in all_res[k].get("phases", {}).get("optimize", {}):
            if not e.startswith("_") and e not in exp_ids:
                exp_ids.append(e)
    lines.append("| experiment | " + " | ".join(disp[k] for k in keys) + " |")
    lines.append("|---|" + "---:|" * len(keys))
    for e in sorted(exp_ids):
        row = [f"`{e}`"]
        for k in keys:
            x = all_res[k].get("phases", {}).get("optimize", {}).get(e)
            if not isinstance(x, dict):
                row.append("·")
            elif x.get("speedup_vs_first"):
                row.append(f"x{x['speedup_vs_first']} ({x['best_variant']})")
            elif "variants" in x and x["variants"] and "rows_per_s" in x["variants"][0]:
                row.append(" / ".join(f"{v.get('rows_per_s') or 'ERR'}" for v in x["variants"]) + " docs/s")
            elif x.get("status") == "n/a":
                row.append("n/a")
            elif "variants" in x and x["variants"]:
                row.append(" / ".join(f"{v.get('p50_ms', 'n/a')}" for v in x["variants"]) + " ms")
            else:
                row.append("-")
        lines.append("| " + " | ".join(row) + " |")
    # --- connections
    lines.append("\n## Connections\n")
    lines.append("| engine | max conn | connect p50 ms | storm 200: opened/failed | 1 worker ops/s | 32 workers ops/s | 32w p99 ms | 64 workers ops/s | CAS contention (applied/rejected, atomic) |")
    lines.append("|---|---:|---:|---|---:|---:|---:|---:|---|")
    for k in keys:
        c = all_res[k].get("phases", {}).get("connections", {})
        if not c:
            continue
        cl = c.get("connect_latency", {})
        direct = cl.get("primary") or next(iter(cl.values()), {})
        storms = (c.get("storm", {}).get("primary") or next(iter(c.get("storm", {}).values()), [])) if c.get("storm") else []
        s200 = next((s for s in storms if s["requested"] == 200), storms[-1] if storms else None)
        conc = c.get("concurrency", {}).get("primary") or next(iter(c.get("concurrency", {}).values()), [])
        w1 = next((x for x in conc if x.get("workers") == 1), {})
        w32 = next((x for x in conc if x.get("workers") == 32), {})
        w64 = next((x for x in conc if x.get("workers") == 64), {})
        wc = c.get("write_contention", {}) or {}
        cas = f"{wc.get('applied')}/{wc.get('rejected')} {'atomic' if wc.get('atomic') else ('NOT atomic' if 'atomic' in wc else wc.get('status', '-'))}" if wc else "-"
        lines.append(f"| {disp[k]} | {c.get('max_connections', '-')} | {_fmt_ms(direct.get('ms', {}).get('p50'))} | "
                     f"{f'{s200['opened']}/{s200['failed']}' if s200 else '-'} | {w1.get('ops_per_s', '-')} | {w32.get('ops_per_s', '-')} | "
                     f"{_fmt_ms(w32.get('latency_ms', {}).get('p99'))} | {w64.get('ops_per_s', '-')} | {cas} |")
    # --- replication
    lines.append("\n## Replication / scaling\n")
    lines.append("| engine | kind | visibility lag p50 / p95 / max ms | replica rejects writes | catch-up after 20k docs (ms) | read scaling gain | failover downtime ms |")
    lines.append("|---|---|---|---|---|---:|---:|")
    for k in keys:
        r = all_res[k].get("phases", {}).get("replication", {})
        if not r:
            continue
        vl = r.get("visibility_lag", {})
        v = next(iter(vl.values()), {}) if vl else {}
        lag = v.get("lag_ms", {}) if isinstance(v, dict) else {}
        wr = r.get("write_rejection", {})
        w = next(iter(wr.values()), {}) if wr else {}
        lul = r.get("lag_under_load", {})
        cu = lul.get("catchup_ms_after_last_write", {}) if isinstance(lul, dict) else {}
        rs = r.get("read_scaling", {})
        fo = r.get("failover", {})
        lines.append(f"| {disp[k]} | {r.get('kind', '-')} | "
                     f"{(_fmt_ms(lag.get('p50')) + ' / ' + _fmt_ms(lag.get('p95')) + ' / ' + _fmt_ms(lag.get('max'))) if lag else (r.get('status') or '-')} | "
                     f"{('yes ' + str(w.get('error_code') or '')) if w.get('writes_rejected') else ('no' if w else '-')} | "
                     f"{next(iter(cu.values()), '-') if cu else '-'} | {rs.get('gain', '-') if isinstance(rs, dict) else '-'} | {fo.get('downtime_ms', '-') if isinstance(fo, dict) else '-'} |")
    # --- load test
    lt_keys = [k for k in keys if isinstance(all_res[k].get("phases", {}).get("loadtest"), dict) and all_res[k]["phases"]["loadtest"].get("runs")]
    if lt_keys:
        lines.append("\n## Multi-connection bulk-insert load test (docs/s; batch size and duration per engine in latest.json)\n")
        lines.append("| engine | entry point | connections | docs/s | batch p50 ms | errors | busiest container (avg cores during inserts) |")
        lines.append("|---|---|---:|---:|---:|---:|---|")
        for k in lt_keys:
            for r in all_res[k]["phases"]["loadtest"]["runs"]:
                usage = r.get("container_usage") or {}
                busiest = max(usage.items(), key=lambda kv: kv[1].get("cpu_cores_avg_during_inserts", 0), default=(None, {}))
                b = f"{busiest[0].replace('nslab-', '')} {busiest[1].get('cpu_cores_avg_during_inserts', 0):.2f}" if busiest[0] else "-"
                lines.append(f"| {disp[k]} | {r.get('target')} | {r.get('workers')} | {r.get('rows_per_s')} | {_fmt_ms(r.get('batch_ms', {}).get('p50'))} | {r.get('errors')} | {b} |")
    # --- backup & restore
    bk_keys = [k for k in keys if isinstance(all_res[k].get("phases", {}).get("backup"), dict) and all_res[k]["phases"]["backup"].get("backup")]
    if bk_keys:
        lines.append("\n## Backup & restore (one full backup with the engine's own tooling, then a verified restore)\n")
        lines.append("| engine | backup method | online | backup s | artifact | % of data | point-read p50 during / baseline ms | restore method | restore s | downtime ms | verified | incremental | DR drill (export → down -v → fresh up → import → restore) |")
        lines.append("|---|---|:---:|---:|---:|---:|---|---|---:|---:|:---:|---|---|")
        for k in bk_keys:
            b = all_res[k]["phases"]["backup"]
            bb, rr, inc = b.get("backup", {}), b.get("restore", {}) or {}, b.get("incremental", {}) or {}
            dur = (bb.get("reads_during") or {}).get("latency_ms", {}).get("p50")
            base = (b.get("reads_baseline") or {}).get("latency_ms", {}).get("p50")
            down = (rr.get("availability") or {}).get("downtime_ms", rr.get("downtime_ms"))
            inc_s = (f"{_fmt_bytes(inc.get('artifact_bytes_delta'))} in {inc.get('seconds')} s" if inc.get("status") == "ok" else (inc.get("status") or "-"))
            dr = b.get("dr") or {}
            drr = dr.get("restore") or {}
            dr_s = ("-" if not dr else (f"✅ verified — export {_fmt_bytes((dr.get('export') or {}).get('bytes'))}, fresh up {(dr.get('fresh_up') or {}).get('seconds', '-')} s, restore {drr.get('seconds', '-')} s"
                                     if drr.get("verified") else f"❌ {dr.get('_error') or drr.get('checks') or 'not verified'}"))
            lines.append(f"| {disp[k]} | {bb.get('method', bb.get('status'))} | {'yes' if bb.get('online') else ('no (' + str(bb.get('downtime_ms')) + ' ms down)' if bb.get('online') is False else '-')} | "
                         f"{bb.get('seconds', '-')} | {_fmt_bytes(bb.get('artifact_bytes'))} | {bb.get('artifact_pct_of_data', '-')} | {_fmt_ms(dur)} / {_fmt_ms(base)} | "
                         f"{rr.get('method', rr.get('status', '-'))} | {rr.get('seconds', '-')} | {_fmt_ms(down) if isinstance(down, (int, float)) else '-'} | "
                         f"{'✅' if rr.get('verified') else ('❌' if rr.get('status') == 'ok' else rr.get('status', '-'))} | {inc_s} | {dr_s} |")
        lines.append("\nStrategy per engine (tool · consistency · restore · incremental · PITR · schedule · retention): see `phases.backup.strategy` in each `latest.json` and `docs/backup-logging.md`.")
    # --- logging
    lg_keys = [k for k in keys if isinstance(all_res[k].get("phases", {}).get("logging"), dict) and "slow_query" in all_res[k]["phases"]["logging"]]
    if lg_keys:
        lines.append("\n## Logging (container log driver, server log format, slow-query log, audit log, run-time log level)\n")
        lines.append("| engine | container logs (driver / rotation / logged so far) | server log | slow-query log | slow op ms → logged as | audit log | run-time level change |")
        lines.append("|---|---|---|---|---|---|---|")
        for k in lg_keys:
            l = all_res[k]["phases"]["logging"]
            cs = l.get("containers", {})
            tot = sum(c.get("bytes", 0) for c in cs.values() if isinstance(c, dict) and "error" not in c)
            fmts = {c.get("format") for c in cs.values() if isinstance(c, dict) and "error" not in c}
            drv = next((f"{c.get('driver')} {c.get('options', {}).get('max-size', '')}x{c.get('options', {}).get('max-file', '')}" for c in cs.values() if isinstance(c, dict) and "error" not in c), "n/a (embedded)")
            sl = (l.get("server_log") or {}).get("value") or {}
            sq = l.get("slow_query", {})
            st = sq.get("setup", {})
            sq_s = (f"✅ {sq.get('effective_threshold_ms')} ms threshold" if sq.get("verified") else ("⚠️ not verified" if st.get("status") == "ok" else f"❌ {st.get('detail', st.get('error', ''))[:60]}"))
            so = f"{(sq.get('slow_op') or {}).get('ms', '-')} → {sq.get('logged_ms', '-')}" if st.get("status") == "ok" else "-"
            au = l.get("audit", {})
            au_s = (f"✅ {au.get('entries_count')} entries" if au.get("verified") else ("⚠️ 0 entries" if (au.get("setup") or {}).get("status") == "ok" else f"❌ {(au.get('setup') or {}).get('detail', '')[:60]}"))
            rl = l.get("runtime_log_level", {})
            rl_s = "✅ " + str(rl.get("value", ""))[:70] if rl.get("status") == "ok" else f"❌ {rl.get('detail', rl.get('error', ''))[:60]}"
            lines.append(f"| {disp[k]} | {drv} / {', '.join(sorted(f for f in fmts if f))} / {_fmt_bytes(tot)} | {sl.get('format', '-')} @ {str(sl.get('where', '-'))[:50]} | {sq_s} | {so} | {au_s} | {rl_s} |")
    lines.append("\n## Notes per engine\n")
    for k in keys:
        r = all_res[k]
        errs = r.get("phases", {}).get("load", {}).get("errors", [])
        lines.append(f"- **{disp[k]}**: {r.get('notes') or ''} {('load errors: ' + '; '.join(errs)) if errs else ''}")
    md = RESULTS / "SUMMARY.md"
    md.write_text("\n".join(lines) + "\n")
    js = RESULTS / "summary.json"
    dump_json(js, {"generated": now_iso(), "engines": {k: {kk: vv for kk, vv in v.items() if kk != "phases"} | {"phases": {p: _strip(pv) for p, pv in v.get("phases", {}).items()}} for k, v in all_res.items()}})
    return md, js


def _strip(v: Any) -> Any:
    if isinstance(v, dict):
        return {k: _strip(x) for k, x in v.items() if k not in ("samples", "samples_ms", "schema", "setup", "params_sample")}
    if isinstance(v, list):
        return [_strip(x) for x in v]
    return v
