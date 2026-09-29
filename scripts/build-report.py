#!/usr/bin/env python3
"""Render results/<engine>/latest.json into a single self-contained HTML report (docs/report.html).

No external data files: the compacted results are embedded as a JSON blob and rendered by inline JS.
Run: uv run --project harness python scripts/build-report.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "harness"))
from nslab.ops import CATALOG                       # noqa: E402
from nslab.probes import PROBES                     # noqa: E402
from nslab.report import load_all, _status          # noqa: E402


def compact(res: dict) -> dict:
    out = {}
    for k, r in res.items():
        ph = r.get("phases", {})
        load = ph.get("load", {})
        bench = {op: {"status": _status(b), "p50": b.get("timing", {}).get("p50"), "p95": b.get("timing", {}).get("p95"),
                      "rows": b.get("rows"), "error": (b.get("error") or "")[:160]}
                 for op, b in ph.get("bench", {}).items() if not op.startswith("_") and isinstance(b, dict)}
        caps = {n: {"status": _status(c), "detail": (c.get("detail") or c.get("error") or "")[:240]}
                for n, c in ph.get("capabilities", {}).items() if isinstance(c, dict)}
        opt = {}
        for e, x in ph.get("optimize", {}).items():
            if not isinstance(x, dict) or e.startswith("_"):
                continue
            opt[e] = {"title": x.get("title"), "what": x.get("what"), "speedup": x.get("speedup_vs_first"), "best": x.get("best_variant"),
                      "status": x.get("status"),
                      "variants": [{"name": v.get("name"), "p50_ms": v.get("p50_ms"), "rows_per_s": v.get("rows_per_s"),
                                    "ok": v.get("ok"), "status": v.get("status"), "note": v.get("note")} for v in x.get("variants", [])]}
        conn = ph.get("connections", {})
        conn_c = {}
        if isinstance(conn, dict) and conn:
            conn_c = {"max_connections": conn.get("max_connections"),
                      "connect_latency": {t: v.get("ms", {}).get("p50") for t, v in conn.get("connect_latency", {}).items() if isinstance(v, dict)},
                      "storm": {t: [{"n": s["requested"], "opened": s["opened"], "failed": s["failed"], "codes": s.get("error_codes")} for s in ss] for t, ss in conn.get("storm", {}).items()},
                      "concurrency": {t: [{"workers": x.get("workers"), "ops_per_s": x.get("ops_per_s"), "p50": x.get("latency_ms", {}).get("p50"), "p99": x.get("latency_ms", {}).get("p99"), "errors": x.get("errors")} for x in xs if "ops_per_s" in x] for t, xs in conn.get("concurrency", {}).items()},
                      "write_contention": conn.get("write_contention")}
        rep = ph.get("replication", {})
        rep_c = {}
        if isinstance(rep, dict) and rep:
            rep_c = {"kind": rep.get("kind"), "status": rep.get("status"),
                     "visibility": {t: v.get("lag_ms") for t, v in rep.get("visibility_lag", {}).items() if isinstance(v, dict)},
                     "write_rejection": {t: {"rejected": v.get("writes_rejected"), "code": v.get("error_code"), "note": v.get("note")} for t, v in rep.get("write_rejection", {}).items() if isinstance(v, dict)},
                     "catchup": (rep.get("lag_under_load") or {}).get("catchup_ms_after_last_write"),
                     "read_scaling": {"gain": (rep.get("read_scaling") or {}).get("gain")},
                     "failover": {kk: vv for kk, vv in (rep.get("failover") or {}).items() if kk in ("downtime_ms", "new_primary", "verified_via", "promote_cmd", "write_attempts_until_success", "kind", "killed", "note", "error", "writes_still_succeed_after_kill")}}
        lt = ph.get("loadtest", {})
        lt_c = {}
        if isinstance(lt, dict) and lt.get("runs"):
            lt_c = {"batch_rows": lt.get("batch_rows"), "seconds": lt.get("seconds_per_run"), "best": lt.get("best"),
                    "runs": [{"target": r.get("target"), "workers": r.get("workers"), "rows_per_s": r.get("rows_per_s"), "errors": r.get("errors"),
                              "p50": r.get("batch_ms", {}).get("p50"), "p95": r.get("batch_ms", {}).get("p95"),
                              "usage": {n.replace("nslab-", ""): {"cores": u.get("cpu_cores_avg_during_inserts", u.get("cpu_cores_avg")), "mem_peak_mb": u.get("mem_peak_mb")}
                                        for n, u in (r.get("container_usage") or {}).items()}} for r in lt["runs"]]}
        elif isinstance(lt, dict) and lt.get("status") == "n/a":
            lt_c = {"na": lt.get("note")}
        bk = ph.get("backup", {})
        bk_c = {}
        if isinstance(bk, dict) and bk.get("backup"):
            bb, rr, inc = bk.get("backup", {}), bk.get("restore", {}) or {}, bk.get("incremental", {}) or {}
            bk_c = {"strategy": bk.get("strategy", {}), "data_bytes": bk.get("data_bytes"),
                    "backup": {kk: bb.get(kk) for kk in ("method", "online", "seconds", "artifact_bytes", "artifact_pct_of_data", "downtime_ms", "status", "detail", "error", "fork_ms", "slm", "server_ms", "files")},
                    "reads_baseline_p50": (bk.get("reads_baseline") or {}).get("latency_ms", {}).get("p50"),
                    "reads_during_p50": (bb.get("reads_during") or {}).get("latency_ms", {}).get("p50"),
                    "reads_during_errors": (bb.get("reads_during") or {}).get("errors"),
                    "restore": {kk: rr.get(kk) for kk in ("method", "online", "seconds", "verified", "status", "detail", "error", "checks", "summary")} | {"downtime_ms": (rr.get("availability") or {}).get("downtime_ms", rr.get("downtime_ms"))},
                    "incremental": {kk: inc.get(kk) for kk in ("status", "method", "seconds", "artifact_bytes_delta", "files_delta", "files", "detail")},
                    "dr": ({"verified": ((bk.get("dr") or {}).get("restore") or {}).get("verified"), "error": (bk.get("dr") or {}).get("_error"),
                            "export_bytes": ((bk.get("dr") or {}).get("export") or {}).get("bytes"), "export_s": ((bk.get("dr") or {}).get("export") or {}).get("seconds"),
                            "destroy": ((bk.get("dr") or {}).get("destroy") or {}).get("method"), "fresh_up_s": ((bk.get("dr") or {}).get("fresh_up") or {}).get("seconds"),
                            "restore_s": ((bk.get("dr") or {}).get("restore") or {}).get("seconds"), "after_destroy": (bk.get("dr") or {}).get("after_destroy"),
                            "seconds": (bk.get("dr") or {}).get("_seconds")} if bk.get("dr") else None)}
        lg = ph.get("logging", {})
        lg_c = {}
        if isinstance(lg, dict) and "slow_query" in lg:
            cs = {c.replace("nslab-", ""): {kk: v.get(kk) for kk in ("driver", "options", "bytes", "lines", "format")} for c, v in (lg.get("containers") or {}).items() if isinstance(v, dict) and "error" not in v}
            sq, au, rl = lg.get("slow_query", {}) or {}, lg.get("audit", {}) or {}, lg.get("runtime_log_level", {}) or {}
            lg_c = {"strategy": lg.get("strategy", {}), "containers": cs,
                    "server_log": {kk: ((lg.get("server_log") or {}).get("value") or {}).get(kk) for kk in ("format", "where")},
                    "slow_query": {"status": (sq.get("setup") or {}).get("status"), "setup": str((sq.get("setup") or {}).get("value") or (sq.get("setup") or {}).get("detail") or (sq.get("setup") or {}).get("error") or "")[:220],
                                   "threshold_ms": sq.get("effective_threshold_ms"), "slow_op": (sq.get("slow_op") or {}).get("desc"), "slow_op_ms": (sq.get("slow_op") or {}).get("ms"),
                                   "fast_op_ms": (sq.get("fast_op") or {}).get("ms"), "logged_ms": sq.get("logged_ms"), "verified": sq.get("verified"),
                                   "entries": [{"ms": e.get("ms"), "text": str(e.get("text"))[:160]} for e in (sq.get("entries") or [])[:4]]},
                    "audit": {"status": (au.get("setup") or {}).get("status"), "setup": str((au.get("setup") or {}).get("value") or (au.get("setup") or {}).get("detail") or "")[:200],
                              "count": au.get("entries_count"), "verified": au.get("verified"), "entries": [str(e)[:160] for e in (au.get("entries") or [])[:3]]},
                    "runtime_level": {"status": rl.get("status"), "text": str(rl.get("value") or rl.get("detail") or rl.get("error") or "")[:200]},
                    "volume": lg.get("log_volume_this_phase")}
        out[k] = {"display": r.get("display", k), "family": r.get("family"), "category": r.get("category"), "image": r.get("image"),
                  "version": str(load.get("server_version") or r.get("server_version") or "")[:70], "notes": r.get("notes"),
                  "load": {"events_rps": (load.get("collections", {}).get("events") or {}).get("rows_per_s"), "load_seconds": load.get("load_seconds"),
                           "collections": {c: {"rps": v.get("rows_per_s"), "s": v.get("seconds"), "method": v.get("method")} for c, v in load.get("collections", {}).items() if isinstance(v, dict)},
                           "errors": load.get("errors", [])},
                  "bench": bench, "caps": caps, "opt": opt, "conn": conn_c, "rep": rep_c, "lt": lt_c, "bk": bk_c, "lg": lg_c}
    return out


SCENARIOS = {
    "messaging": "Chatter — social messaging store (register / find-friends + 1:1 & group chat)",
    "recgraph": "GraphRec — product recommendations (co-purchase) + fraud-ring / identity detection",
}


def load_scenarios() -> dict:
    """Collect per engine, per scenario: seed summary, steady-state run files (each with its own opts), and curve files."""
    from nslab.util import RESULTS, load_json
    out: dict = {}
    if not RESULTS.exists():
        return out
    dirs = [(d, d.name) for d in sorted(RESULTS.iterdir()) if d.is_dir() and d.name != "k3s"]
    if (RESULTS / "k3s").exists():
        dirs += [(d, f"{d.name}@k3s") for d in sorted((RESULTS / "k3s").iterdir()) if d.is_dir()]
    for d, dkey in dirs:
        for scen in SCENARIOS:
            # newest run/curve per distinct opts signature (e.g. messaging fan-out write vs read)
            runs, curves = {}, {}
            for f in sorted(d.glob(f"scenario-{scen}-2*.json")):
                j = load_json(f)
                if j.get("runs"):
                    runs[str(j.get("opts") or j.get("fanout") or "default")] = j
            for f in sorted(d.glob(f"scenario-{scen}-curves-2*.json")):
                j = load_json(f)
                curves[str(j.get("opts") or "default")] = j
            seed = None
            sf = d / f"scenario-{scen}-seed-latest.json"
            if sf.exists():
                seed = load_json(sf)
            if runs or seed:
                out.setdefault(dkey, {})[scen] = {"blurb": SCENARIOS[scen] + (" — on k3s" if dkey.endswith("@k3s") else ""), "seed": seed,
                                                  "runs": list(runs.values()), "curves": list(curves.values())}
    return out


def build() -> Path:
    data = {
        "generated": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(timespec="seconds"),
        "engines": compact(load_all()),
        "ops": [{"id": o.id, "title": o.title, "kind": o.kind, "tags": list(o.tags), "contract": o.contract} for o in CATALOG],
        "probes": [{"name": p.name, "title": p.title, "group": p.group} for p in PROBES],
        "scenarios": load_scenarios(),
    }
    full = TEMPLATE.replace("/*DATA*/", json.dumps(data, separators=(",", ":")))
    out = ROOT / "docs" / "report.html"
    out.parent.mkdir(exist_ok=True)
    # standalone: font link into <head>
    out.write_text(full.replace("<title>", FONTLINK + "\n<title>", 1))
    # artifact: content only (no <!doctype>/<html>/<head>/<body>); title + style + body + scripts, font link first
    i, j = full.index("<title>"), full.index("</body>")
    art = FONTLINK + "\n" + full[i:j].replace("</head>", "").replace("<body>", "") + "</body>"
    (ROOT / "docs" / "report.artifact.html").write_text(art)
    return out


FONTLINK = '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&display=swap">'


TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NoSQL Database Lab</title>
<style>
:root{
  --ground:#F6F7F9; --panel:#FFFFFF; --ink:#171F27; --ink-2:#4C5A66; --muted:#7A8894; --line:#DBE1E7; --line-2:#EDF0F3;
  --accent:#5B4BC4; --accent-ink:#4636A8; --accent-soft:#E9E6F8;
  --ok:#2F8A5A; --warn:#C48A1C; --bad:#C0463C; --na:#9AA6B0;
  --ok-soft:#E4F3EA; --warn-soft:#FAF0DA; --bad-soft:#F8E4E2;
  --h0:#FFFFFF; --h1:#EDEAFA; --h2:#D2CCF2; --h3:#B0A6E6; --h4:#8A7DD6; --h5:#6455C4;
  --shadow:0 1px 2px rgba(23,31,39,.06),0 8px 24px -16px rgba(23,31,39,.25);
}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){
  --ground:#0F1317; --panel:#161B21; --ink:#E6EBF0; --ink-2:#AEBAC4; --muted:#7E8C97; --line:#2A343D; --line-2:#1E262D;
  --accent:#9A8CF0; --accent-ink:#B6ABF7; --accent-soft:#241E43;
  --ok:#5CC48A; --warn:#E0A83B; --bad:#E5716A; --na:#63707B;
  --ok-soft:#153426; --warn-soft:#3A2D10; --bad-soft:#3E1D1A;
  --h0:#161B21; --h1:#20203A; --h2:#2C2A55; --h3:#3B3675; --h4:#4E4796; --h5:#6455C4;
  --shadow:0 1px 2px rgba(0,0,0,.4),0 8px 24px -16px rgba(0,0,0,.6);
}}
:root[data-theme="dark"]{
  --ground:#0F1317; --panel:#161B21; --ink:#E6EBF0; --ink-2:#AEBAC4; --muted:#7E8C97; --line:#2A343D; --line-2:#1E262D;
  --accent:#9A8CF0; --accent-ink:#B6ABF7; --accent-soft:#241E43;
  --ok:#5CC48A; --warn:#E0A83B; --bad:#E5716A; --na:#63707B;
  --ok-soft:#153426; --warn-soft:#3A2D10; --bad-soft:#3E1D1A;
  --h0:#161B21; --h1:#20203A; --h2:#2C2A55; --h3:#3B3675; --h4:#4E4796; --h5:#6455C4;
}
*{box-sizing:border-box}
body{margin:0;background:var(--ground);color:var(--ink);font-family:ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;font-size:14.5px;line-height:1.55;padding:0 clamp(14px,4vw,44px) 72px}
h1,h2,h3{font-weight:650;letter-spacing:-.01em;margin:0}
h1{font-family:"IBM Plex Mono",ui-monospace,Menlo,Consolas,monospace;font-size:clamp(26px,4vw,40px);line-height:1.05;font-weight:600;letter-spacing:-.02em}
h2{font-size:22px;margin-top:52px;padding-top:22px;border-top:1px solid var(--line)}
h3{font-size:16px;margin-top:26px;color:var(--ink-2)}
a{color:var(--accent-ink)}
header{padding:40px 0 8px;border-bottom:1px solid var(--line)}
.eyebrow{font-family:"IBM Plex Mono",ui-monospace,monospace;font-size:12px;text-transform:uppercase;letter-spacing:.18em;color:var(--accent-ink);margin-bottom:12px}
.eyebrow .dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--accent);margin-right:8px;vertical-align:middle}
.sub{color:var(--muted);max-width:70ch}
.lede{color:var(--ink-2);max-width:74ch;margin-top:14px}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:12px;margin:24px 0 8px}
.tile{border:1px solid var(--line);border-radius:12px;background:var(--panel);padding:14px 16px;box-shadow:var(--shadow)}
.tile .num{font-family:"IBM Plex Mono",ui-monospace,monospace;font-size:30px;font-weight:600;line-height:1;letter-spacing:-.02em;color:var(--ink);font-variant-numeric:tabular-nums}
.tile .lbl{font-family:"IBM Plex Mono",ui-monospace,monospace;font-size:11px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-top:8px}
.tile.accent{border-color:var(--h3);background:var(--accent-soft)}
.tile.accent .num{color:var(--accent-ink)}
.wrap{overflow-x:auto;margin-top:16px;border:1px solid var(--line);border-radius:12px;background:var(--panel);box-shadow:var(--shadow)}
table{border-collapse:collapse;width:100%;font-size:13px}
th,td{padding:7px 10px;text-align:left;white-space:nowrap;border-bottom:1px solid var(--line-2)}
thead th{position:sticky;top:0;background:var(--panel);border-bottom:1px solid var(--line);font-weight:600;color:var(--ink-2);z-index:2}
tbody tr:hover{background:var(--accent-soft)}
td.n,th.n{text-align:right;font-variant-numeric:tabular-nums}
td.name{font-weight:600}
.tag{display:inline-block;font-size:11px;padding:1px 7px;border-radius:999px;background:var(--accent-soft);color:var(--accent-ink);border:1px solid var(--line)}
.fam{font-family:"IBM Plex Mono",ui-monospace,monospace;font-size:11px;text-transform:uppercase;letter-spacing:.04em;color:var(--muted)}
.cell{border-radius:5px;padding:5px 8px;text-align:right;font-variant-numeric:tabular-nums}
.sym{text-align:center;font-size:15px}
.ok{color:var(--ok)} .bad{color:var(--bad)} .warn{color:var(--warn)} .na{color:var(--na)}
.mono{font-family:"IBM Plex Mono",ui-monospace,"SF Mono",Menlo,Consolas,monospace;font-size:12px}
.cell,td.n,.tile .num{font-family:"IBM Plex Mono",ui-monospace,Menlo,Consolas,monospace}
.muted{color:var(--muted)}
.card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:16px 18px;box-shadow:var(--shadow);margin-top:16px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:16px;margin-top:16px}
.kv{display:flex;justify-content:space-between;gap:12px;padding:3px 0;border-bottom:1px dashed var(--line-2);font-size:13px}
.kv .k{color:var(--muted)} .kv .v{font-variant-numeric:tabular-nums;text-align:right}
.bar{height:8px;border-radius:4px;background:var(--accent);opacity:.85}
.legend{display:flex;gap:14px;flex-wrap:wrap;color:var(--muted);font-size:12px;margin-top:10px}
.legend span{display:inline-flex;align-items:center;gap:5px}
.sw{width:13px;height:13px;border-radius:3px;display:inline-block;border:1px solid var(--line)}
details{margin-top:8px} summary{cursor:pointer;color:var(--accent-ink);font-size:13px}
.pill{font-size:11px;padding:1px 6px;border-radius:5px;border:1px solid var(--line)}
.toggle{position:fixed;top:12px;right:14px;z-index:9;background:var(--panel);border:1px solid var(--line);border-radius:8px;color:var(--ink);padding:6px 10px;cursor:pointer;font-size:12px;box-shadow:var(--shadow)}
footer{margin-top:60px;color:var(--muted);font-size:12px;border-top:1px solid var(--line);padding-top:18px}
</style>
</head>
<body>
<button class="toggle" id="themeBtn">◐ theme</button>
<header>
  <div class="eyebrow"><span class="dot"></span>ns-databases · a reproducible boilerplate</div>
  <h1>NoSQL Database Lab</h1>
  <p class="sub" id="sub"></p>
  <p class="lede">One deterministic e-commerce dataset (the same values as the relational lab <span class="mono">rd-databases</span>) loaded into every engine, then a shared <b>operation catalog</b> timed against each: point &amp; batch reads, writes, partial updates, atomic increments, compare-and-set, range / secondary / nested / array queries, top-N, offset vs keyset pagination, group-by and time-bucket aggregation, joins, graph traversal, full-text, geo radius, vector kNN, multi-document transactions and TTL. An operation an engine has no way to express is marked <span class="na">n/a</span>; one that fails at run time <span class="bad">ERR</span>.</p>
  <div class="tiles" id="tiles"></div>
</header>
<main id="app"></main>
<footer>
  <p>Generated <span id="gen"></span> from <span class="mono">results/&lt;engine&gt;/latest.json</span>. p50 = client-observed median latency (monotonic clock, warm cache, ≥10 iterations after 2 warm-ups, result-set checksum recorded). Throughput figures use forked multi-process workers. Numbers come from one 28-core / 125 GB host with every container on that machine; some engines were measured with light cross-engine CPU contention (re-run serially with <span class="mono">scripts/run-all.sh</span> for publication-grade numbers).</p>
</footer>
<script id="data" type="application/json">/*DATA*/</script>
<script>
const D = JSON.parse(document.getElementById('data').textContent);
const engines = Object.keys(D.engines).sort((a,b)=> (D.engines[a].family+D.engines[a].display).localeCompare(D.engines[b].family+D.engines[b].display));
const E = k => D.engines[k];
const el = (t,c,h)=>{const e=document.createElement(t); if(c)e.className=c; if(h!==undefined)e.innerHTML=h; return e;};
const fmtms = v => v==null?'':(v>=1000?(v/1000).toFixed(2)+' s':v>=10?Math.round(v):v.toFixed(2));
const fmtn = v => v==null?'':v.toLocaleString();
const esc = s => String(s??'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

// heat scale over log10(ms)
function heat(v){ if(v==null) return 'var(--h0)'; const l=Math.log10(Math.max(v,0.01)); // -2..4
  const t=Math.max(0,Math.min(1,(l+2)/6)); const stops=['--h0','--h1','--h2','--h3','--h4','--h5'];
  return `var(${stops[Math.min(5,Math.floor(t*5))]})`; }
function textColorFor(v){ if(v==null) return 'var(--muted)'; return v>=1000?'#fff':'inherit'; }

const app = document.getElementById('app');
const nFamilies = new Set(engines.map(k=>E(k).family)).size;
document.getElementById('sub').textContent = `${engines.length} engines across ${nFamilies} families, each run through nine phases (load, capabilities, bench, optimise, connections, load test, replication, backup/restore, logging) and one domain scenario.`;
document.getElementById('gen').textContent = D.generated;
(function(){
  const scEng = Object.keys(D.scenarios||{}).filter(k=>(D.scenarios[k].runs&&Object.keys(D.scenarios[k].runs).length));
  const tiles=[[engines.length,'engines'],[nFamilies,'families'],[D.ops.length,'operations'],[D.probes.length,'capability probes']];
  if(scEng.length){const sc=D.scenarios[scEng[0]]; const wr=sc.runs.write; const peak=wr&&wr.peak_throughput_ops_s;
    tiles.push([peak?fmtn(peak):'1','ops/s peak · Chatter scenario', true]);}
  const box=document.getElementById('tiles');
  for(const [n,l,acc] of tiles){const t=el('div','tile'+(acc?' accent':'')); t.innerHTML=`<div class="num">${typeof n==='number'?fmtn(n):n}</div><div class="lbl">${l}</div>`; box.appendChild(t);}
})();

function section(title, node){ app.appendChild(el('h2',null,title)); app.appendChild(node); }

// --- engines table
(function(){
  const w=el('div','wrap'); const t=el('table');
  t.innerHTML='<thead><tr><th>engine</th><th>family</th><th>version</th><th class="n">load events/s</th><th class="n">load s</th><th>replication</th></tr></thead>';
  const tb=el('tbody');
  for(const k of engines){const e=E(k); const tr=el('tr');
    tr.innerHTML=`<td class="name">${e.display}</td><td><span class="fam">${e.family||''}</span><div class="muted" style="font-size:11px">${e.category||''}</div></td>`+
      `<td class="mono" style="max-width:280px;white-space:normal">${e.version||''}</td><td class="n">${fmtn(e.load.events_rps)}</td><td class="n">${e.load.load_seconds??''}</td>`+
      `<td style="white-space:normal;max-width:260px" class="muted">${(e.rep&&e.rep.kind)||'—'}</td>`;
    tb.appendChild(tr);}
  t.appendChild(tb); w.appendChild(t); section('Engines', w);
})();

// --- operation matrix (heatmap)
(function(){
  const w=el('div','wrap'); const t=el('table');
  let head='<thead><tr><th>operation</th>'+engines.map(k=>`<th class="n" title="${E(k).category||''}">${E(k).display}</th>`).join('')+'</tr></thead>';
  t.innerHTML=head; const tb=el('tbody');
  for(const op of D.ops){const tr=el('tr');
    tr.appendChild(el('td','', `<span class="mono">${op.id}</span><div class="muted" style="font-size:11px;white-space:normal;max-width:340px">${op.title}</div>`));
    for(const k of engines){const b=E(k).bench[op.id]; const td=el('td','n');
      if(!b){td.innerHTML='<span class="na">·</span>';}
      else if(b.status==='ok'){const c=el('div','cell'); c.style.background=heat(b.p50); c.style.color=textColorFor(b.p50); c.textContent=fmtms(b.p50); c.title=`p50 ${b.p50} ms · p95 ${b.p95} ms · rows ${b.rows}`; td.appendChild(c);}
      else if(b.status==='unsupported'){td.innerHTML='<span class="na" title="'+(b.error||'')+'">n/a</span>';}
      else {td.innerHTML='<span class="bad" title="'+(b.error||'')+'">ERR</span>';}
      tr.appendChild(td);}
    tb.appendChild(tr);}
  t.appendChild(tb); w.appendChild(t); section('Operation latency & capability matrix (p50)', w);
  const lg=el('div','legend'); lg.innerHTML='<span><span class="sw" style="background:var(--h1)"></span>&lt;0.1ms</span><span><span class="sw" style="background:var(--h2)"></span>~1ms</span><span><span class="sw" style="background:var(--h3)"></span>~10ms</span><span><span class="sw" style="background:var(--h4)"></span>~100ms</span><span><span class="sw" style="background:var(--h5)"></span>≥1s</span><span class="na">n/a = no way to express</span><span class="bad">ERR = failed at run time</span>';
  app.appendChild(lg);
})();

// --- capability probes
(function(){
  const w=el('div','wrap'); const t=el('table');
  t.innerHTML='<thead><tr><th>capability</th>'+engines.map(k=>`<th class="sym">${E(k).display}</th>`).join('')+'</tr></thead>';
  const tb=el('tbody'); const sym={supported:'<span class="ok">✔</span>',unsupported:'<span class="na">—</span>',error:'<span class="bad">⚠</span>'};
  for(const p of D.probes){const tr=el('tr');
    tr.appendChild(el('td','',`${p.title}<div class="muted mono" style="font-size:11px">${p.name} · ${p.group}</div>`));
    for(const k of engines){const c=E(k).caps[p.name]; const td=el('td','sym');
      td.innerHTML=c?(sym[c.status]||'<span class="na">·</span>'):'<span class="na">·</span>'; if(c&&c.detail)td.title=c.detail; tr.appendChild(td);}
    tb.appendChild(tr);}
  t.appendChild(tb); w.appendChild(t); section('Feature probes', w);
})();

// --- optimisation experiments
(function(){
  section('Optimisation experiments', el('div','muted',"Each experiment measures an operation, changes one thing (index, document shape, batching, consistency level, connection handling), and measures again. Speed-up is best variant vs first variant."));
  const g=el('div','grid');
  const expIds=[...new Set(engines.flatMap(k=>Object.keys(E(k).opt)))].sort();
  for(const e of expIds){
    const card=el('div','card'); let title=null,what=null;
    for(const k of engines){const x=E(k).opt[e]; if(x){title=title||x.title; what=what||x.what;}}
    card.appendChild(el('div',null,`<b class="mono">${e}</b>`));
    if(title)card.appendChild(el('div','muted',title));
    for(const k of engines){const x=E(k).opt[e]; if(!x)continue; const row=el('div','kv');
      let val;
      if(x.status==='n/a') val='<span class="na">n/a</span>';
      else if(x.speedup) val=`<b>×${x.speedup}</b> <span class="muted">${x.best}</span>`;
      else if(x.variants&&x.variants[0]&&x.variants[0].rows_per_s!=null) val=x.variants.map(v=>v.rows_per_s!=null?fmtn(v.rows_per_s):(v.status==='n/a'?'n/a':'ERR')).join(' / ')+' <span class="muted">docs/s</span>';
      else if(x.variants&&x.variants.length) val=x.variants.map(v=>v.p50_ms!=null?fmtms(v.p50_ms):(v.status==='n/a'?'n/a':'ERR')).join(' / ')+' <span class="muted">ms</span>';
      else val='—';
      row.innerHTML=`<span class="k">${E(k).display}</span><span class="v">${val}</span>`; card.appendChild(row);}
    g.appendChild(card);
  }
  app.appendChild(g);
})();

// --- connections
(function(){
  const w=el('div','wrap'); const t=el('table');
  t.innerHTML='<thead><tr><th>engine</th><th class="n">max conn</th><th class="n">connect p50 ms</th><th class="n">1 worker ops/s</th><th class="n">16w ops/s</th><th class="n">64w ops/s</th><th>CAS contention</th></tr></thead>';
  const tb=el('tbody');
  for(const k of engines){const c=E(k).conn; if(!c||!c.concurrency)continue;
    const prim=Object.keys(c.concurrency)[0]; const cc=c.concurrency[prim]||[];
    const at=n=>{const x=cc.find(w=>w.workers===n);return x?x.ops_per_s:null;};
    const clat=c.connect_latency?Object.values(c.connect_latency)[0]:null;
    const wc=c.write_contention||{}; let cas='—';
    if(wc.applied!=null)cas=`${wc.applied}/${wc.rejected} ${wc.atomic?'<span class="ok">atomic</span>':(wc.status==='n/a'?'<span class="na">n/a</span>':'<span class="bad">not atomic</span>')}`;
    else if(wc.status)cas=`<span class="na">${wc.status}</span>`;
    const tr=el('tr'); tr.innerHTML=`<td class="name">${E(k).display}</td><td class="n">${c.max_connections??'—'}</td><td class="n">${fmtms(clat)}</td><td class="n">${fmtn(at(1))}</td><td class="n">${fmtn(at(16))}</td><td class="n">${fmtn(at(64))}</td><td>${cas}</td>`;
    tb.appendChild(tr);}
  t.appendChild(tb); w.appendChild(t); section('Connections & concurrency', w);
})();

// --- replication
(function(){
  const rows=engines.filter(k=>E(k).rep&&(E(k).rep.visibility&&Object.keys(E(k).rep.visibility).length||E(k).rep.status));
  if(!rows.length)return;
  const w=el('div','wrap'); const t=el('table');
  t.innerHTML='<thead><tr><th>engine</th><th>kind</th><th class="n">visibility lag p50 ms</th><th>replica rejects writes</th><th class="n">catch-up 20k docs ms</th><th class="n">read-scaling gain</th><th class="n">failover downtime ms</th></tr></thead>';
  const tb=el('tbody');
  for(const k of rows){const r=E(k).rep;
    const vis=r.visibility?Object.values(r.visibility)[0]:null; const vlag=vis?vis.p50:null;
    const wr=r.write_rejection?Object.values(r.write_rejection)[0]:null;
    const wrs=wr?(wr.rejected?('<span class="ok">yes</span> '+(wr.code||'')):'<span class="warn">accepted</span>'):'—';
    const cu=r.catchup?Object.values(r.catchup)[0]:null;
    const fo=r.failover||{};
    const tr=el('tr'); tr.innerHTML=`<td class="name">${E(k).display}</td><td style="white-space:normal;max-width:240px" class="muted">${r.kind||r.status||'—'}</td><td class="n">${vlag!=null?fmtms(vlag):(r.status==='single-node'?'<span class="na">single node</span>':'—')}</td><td>${wrs}</td><td class="n">${cu!=null?fmtms(cu):''}</td><td class="n">${r.read_scaling&&r.read_scaling.gain!=null?'×'+r.read_scaling.gain:''}</td><td class="n">${fo.downtime_ms!=null?fmtn(Math.round(fo.downtime_ms)):(fo.note||'')}</td>`;
    tb.appendChild(tr);}
  t.appendChild(tb); w.appendChild(t); section('Replication & failover', w);
})();

// --- load test
(function(){
  const rows=engines.filter(k=>E(k).lt&&E(k).lt.runs);
  if(!rows.length)return;
  const w=el('div','wrap'); const t=el('table');
  t.innerHTML='<thead><tr><th>engine</th><th>entry point</th><th class="n">workers</th><th class="n">docs/s</th><th class="n">batch p50 ms</th><th class="n">errors</th><th>busiest container (cores)</th></tr></thead>';
  const tb=el('tbody');
  for(const k of rows){for(const r of E(k).lt.runs){
    const busiest=Object.entries(r.usage||{}).sort((a,b)=>(b[1].cores||0)-(a[1].cores||0))[0];
    const tr=el('tr'); tr.innerHTML=`<td class="name">${E(k).display}</td><td class="mono">${r.target}</td><td class="n">${r.workers}</td><td class="n">${fmtn(r.rows_per_s)}</td><td class="n">${fmtms(r.p50)}</td><td class="n">${r.errors||0}</td><td class="muted">${busiest?busiest[0]+' '+(busiest[1].cores||0).toFixed(2):''}</td>`;
    tb.appendChild(tr);}}
  t.appendChild(tb); w.appendChild(t); section('Multi-connection bulk-insert load test', w);
})();

// --- backup & restore
(function(){
  const rows=engines.filter(k=>E(k).bk&&E(k).bk.backup);
  if(!rows.length)return;
  const fmtb=v=>v==null?'—':v>=1e9?(v/1e9).toFixed(2)+' GB':v>=1e6?(v/1e6).toFixed(1)+' MB':v>=1e3?Math.round(v/1e3)+' KB':v+' B';
  section('Backup & restore', el('div','muted','One full backup with the engine\'s own tooling while point reads keep running, then the dataset is mutated (500 probe docs added, 50 customers deleted) and the backup is restored. "Verified" means counts, the deleted customers and the probe collection all match the pre-backup fingerprint and the post-backup writes are gone. Downtime is the longest window in which the primary did not answer. "Survives down -v" is the disaster-recovery drill: the artifact is exported off the stack, the stack is destroyed with docker compose down -v (data and backups volumes), a fresh one is started, the artifact imported and restored, and the fingerprint checked again.'));
  const w=el('div','wrap'); const t=el('table');
  t.innerHTML='<thead><tr><th>engine</th><th>backup</th><th class="n">online</th><th class="n">backup s</th><th class="n">artifact</th><th class="n">% of data</th><th class="n">read p50 during / base ms</th><th>restore</th><th class="n">restore s</th><th class="n">downtime</th><th class="sym">verified</th><th>incremental</th><th title="backup → export off the stack → docker compose down -v → fresh up → import → restore → verify">survives down -v</th></tr></thead>';
  const tb=el('tbody');
  for(const k of rows){const b=E(k).bk; const bb=b.backup, rr=b.restore||{}, inc=b.incremental||{};
    const online=bb.online===true?'<span class="ok">yes</span>':bb.online===false?'<span class="warn">no</span><div class="muted" style="font-size:11px">'+fmtn(Math.round(bb.downtime_ms||0))+' ms down</div>':'—';
    const ver=rr.verified===true?'<span class="ok">✔</span>':rr.status==='ok'?'<span class="bad">✘</span>':'<span class="na" title="'+esc(rr.detail||rr.error||'')+'">'+esc(rr.status||'—')+'</span>';
    const incs=inc.status==='ok'?fmtb(inc.artifact_bytes_delta)+' <span class="muted">in '+inc.seconds+' s</span>':'<span class="na" title="'+esc(inc.detail||'')+'">'+esc(inc.status||'—')+'</span>';
    const dr=b.dr; const drs=!dr?'<span class="na">—</span>':dr.verified?`<span class="ok">✔</span> <span class="muted">export ${fmtb(dr.export_bytes)} · fresh up ${dr.fresh_up_s??0} s · restore ${dr.restore_s} s</span>`:`<span class="bad">✘</span> <span class="muted">${esc(dr.error||'not verified')}</span>`;
    const tr=el('tr'); tr.innerHTML=`<td class="name">${E(k).display}</td><td style="white-space:normal;max-width:260px" class="muted">${esc(bb.method||bb.status||'')}${bb.error?' <span class="bad">'+esc(bb.error)+'</span>':''}</td><td class="n">${online}</td><td class="n">${bb.seconds??''}</td><td class="n">${fmtb(bb.artifact_bytes)}</td><td class="n">${bb.artifact_pct_of_data??''}</td><td class="n">${fmtms(b.reads_during_p50)} / ${fmtms(b.reads_baseline_p50)}</td><td style="white-space:normal;max-width:280px" class="muted">${esc(rr.method||rr.status||'')}${rr.error?' <span class="bad">'+esc(rr.error)+'</span>':''}</td><td class="n">${rr.seconds??''}</td><td class="n">${rr.downtime_ms!=null?fmtms(rr.downtime_ms):''}</td><td class="sym">${ver}</td><td>${incs}</td><td style="white-space:normal;max-width:240px">${drs}</td>`;
    tb.appendChild(tr);}
  t.appendChild(tb); w.appendChild(t); app.appendChild(w);
  const g=el('div','grid');
  for(const k of rows){const st=E(k).bk.strategy||{}; if(!Object.keys(st).length)continue; const card=el('div','card'); card.appendChild(el('div',null,`<b>${E(k).display}</b> <span class="muted">· data ${fmtb(E(k).bk.data_bytes)}</span>`));
    for(const key of ['tool','consistency','restore','incremental','pitr','schedule','retention']){if(!st[key])continue; const row=el('div','kv'); row.style.whiteSpace='normal'; row.innerHTML=`<span class="k" style="min-width:86px">${key}</span><span class="v" style="text-align:left">${esc(st[key])}</span>`; card.appendChild(row);}
    g.appendChild(card);}
  app.appendChild(g);
})();

// --- logging
(function(){
  const rows=engines.filter(k=>E(k).lg&&E(k).lg.slow_query);
  if(!rows.length)return;
  const fmtb=v=>v==null?'—':v>=1e9?(v/1e9).toFixed(2)+' GB':v>=1e6?(v/1e6).toFixed(1)+' MB':v>=1e3?Math.round(v/1e3)+' KB':v+' B';
  section('Logging', el('div','muted','Container log driver and rotation (compose x-logging), the engine\'s own log format and location, then three live checks: a slow-query log (one point read that must not be logged, one known-slow operation that must, both read back through the engine\'s own interface), an audit log round trip, and a log-level change at run time. Log volume = what the engine wrote to stdout so far.'));
  const w=el('div','wrap'); const t=el('table');
  t.innerHTML='<thead><tr><th>engine</th><th>container logs</th><th>server log</th><th>slow-query log</th><th class="n">slow op ms → logged</th><th>audit log</th><th>run-time level</th></tr></thead>';
  const tb=el('tbody');
  for(const k of rows){const l=E(k).lg; const cs=Object.values(l.containers||{}); const tot=cs.reduce((a,c)=>a+(c.bytes||0),0);
    const drv=cs.length?`${cs[0].driver} ${(cs[0].options||{})['max-size']||''}×${(cs[0].options||{})['max-file']||''}<div class="muted" style="font-size:11px">${[...new Set(cs.map(c=>c.format))].join(', ')} · ${fmtb(tot)} · ${cs.length} container${cs.length>1?'s':''}</div>`:'<span class="na">embedded: no container</span>';
    const sq=l.slow_query||{}; const sqs=sq.verified?`<span class="ok">✔</span> ${sq.threshold_ms} ms threshold<div class="muted" style="font-size:11px;white-space:normal;max-width:300px">${esc(sq.setup)}</div>`:sq.status==='ok'?`<span class="warn">not verified</span><div class="muted" style="font-size:11px;white-space:normal;max-width:300px">${esc(sq.setup)}</div>`:`<span class="na">—</span><div class="muted" style="font-size:11px;white-space:normal;max-width:300px">${esc(sq.setup)}</div>`;
    const so=sq.status==='ok'?`${fmtms(sq.slow_op_ms)} → ${sq.logged_ms!=null?fmtms(sq.logged_ms):'<span class="bad">not logged</span>'}<div class="muted" style="font-size:11px">fast op ${fmtms(sq.fast_op_ms)} ms not logged</div>`:'';
    const au=l.audit||{}; const aus=au.verified?`<span class="ok">✔</span> ${au.count} entries<div class="muted" style="font-size:11px;white-space:normal;max-width:280px">${esc(au.setup)}</div>`:au.status==='ok'?`<span class="warn">0 entries</span>`:`<span class="na">—</span><div class="muted" style="font-size:11px;white-space:normal;max-width:280px">${esc(au.setup)}</div>`;
    const rl=l.runtime_level||{}; const rls=rl.status==='ok'?`<span class="ok">✔</span><div class="muted" style="font-size:11px;white-space:normal;max-width:300px">${esc(rl.text)}</div>`:`<span class="na">—</span><div class="muted" style="font-size:11px;white-space:normal;max-width:300px">${esc(rl.text)}</div>`;
    const sl=l.server_log||{};
    const tr=el('tr'); tr.innerHTML=`<td class="name">${E(k).display}</td><td>${drv}</td><td style="white-space:normal;max-width:220px"><b>${esc(sl.format||'—')}</b><div class="muted" style="font-size:11px">${esc(sl.where||'')}</div></td><td>${sqs}</td><td class="n">${so}</td><td>${aus}</td><td>${rls}</td>`;
    tb.appendChild(tr);}
  t.appendChild(tb); w.appendChild(t); app.appendChild(w);
  const g=el('div','grid');
  for(const k of rows){const st=E(k).lg.strategy||{}; if(!Object.keys(st).length)continue; const card=el('div','card'); card.appendChild(el('div',null,`<b>${E(k).display}</b>`));
    for(const key of ['server_log','slow_query','audit','runtime']){if(!st[key])continue; const row=el('div','kv'); row.style.whiteSpace='normal'; row.innerHTML=`<span class="k" style="min-width:86px">${key.replace('_',' ')}</span><span class="v" style="text-align:left">${esc(st[key])}</span>`; card.appendChild(row);}
    const ent=(E(k).lg.slow_query||{}).entries||[]; if(ent.length){const d=el('details'); d.innerHTML='<summary>slow-log entries read back</summary>'+ent.map(e=>`<div class="kv"><span class="k mono" style="white-space:normal">${esc(e.text)}</span><span class="v">${e.ms!=null?fmtms(e.ms)+' ms':''}</span></div>`).join(''); card.appendChild(d);}
    g.appendChild(card);}
  app.appendChild(g);
})();

// --- scenarios (domain workloads beyond the generic catalog)
(function(){
  const S=D.scenarios||{};
  const engs=Object.keys(S); if(!engs.length)return;
  const CURVE_TITLES={send_vs_group_size:'Send latency vs group size',pymk_vs_degree:'PYMK latency vs friend degree',
    reco_vs_popularity:'Recommendation latency vs product popularity',ring_expand_vs_depth:'Fraud ring-expansion latency vs traversal depth'};
  const optLabel=o=>{try{const j=typeof o==='string'&&o.startsWith('{')?JSON.parse(o.replace(/'/g,'"')):o; if(j&&j.fanout)return 'fan-out='+j.fanout; }catch(e){} return '';};
  let any=false;
  for(const k of engs){for(const scen of Object.keys(S[k])){any=true;
    const sc=S[k][scen]; const disp=(D.engines[k]||{}).display||k;
    app.appendChild(el('h2',null,`Scenario — ${sc.blurb.split(' — ')[0]} on ${disp}`));
    app.appendChild(el('div','muted',sc.blurb.split(' — ').slice(1).join(' — ')+' — a weighted, hotspot-skewed, multi-process load beyond the generic operation catalog.'));
    if(sc.seed){const s=sc.seed; const wr=s.written||{}; const parts=Object.entries(wr).map(([kk,vv])=>`${fmtn(vv)} ${kk}`).join(' · ');
      app.appendChild(el('div','muted',`seed: ${parts}${s.messages_per_s?` (${fmtn(s.messages_per_s)} msgs/s)`:''}. ${s.server||''}`));}
    for(const run of sc.runs){
      const ops=Object.keys(run.mix||{}).sort((a,b)=>(run.mix[b]||0)-(run.mix[a]||0));
      const w=el('div','wrap'); const t=el('table');
      const lab=optLabel(run.opts&&Object.keys(run.opts).length?run.opts:'')||'workers';
      t.innerHTML='<thead><tr><th>'+(lab==='workers'?'workers':lab+' · workers')+'</th><th class="n">throughput ops/s</th><th class="n">errors</th>'+ops.map(o=>`<th class="n" title="p95 ms">${o}</th>`).join('')+'</tr></thead>';
      const tb=el('tbody');
      for(const r of run.runs){const tr=el('tr');
        let cells=`<td class="name">${r.workers}</td><td class="n">${fmtn(r.throughput_ops_s)}</td><td class="n">${r.errors||0}</td>`;
        for(const o of ops){const p=r.per_op&&r.per_op[o]; const v=p&&p.latency_ms?p.latency_ms.p95:null;
          const c=v!=null?`<div class="cell" style="background:${heat(v)};color:${textColorFor(v)}">${fmtms(v)}</div>`:'<span class="na">·</span>';
          cells+=`<td class="n">${c}</td>`;}
        tr.innerHTML=cells; tb.appendChild(tr);}
      t.appendChild(tb); w.appendChild(t); app.appendChild(w);
    }
    for(const cv of sc.curves){const g=el('div','grid');
      for(const key of Object.keys(cv)){if(key==='opts'||key==='fanout'||typeof cv[key]!=='object')continue;
        const b=cv[key]; if(!b||typeof b!=='object')continue;
        const card=el('div','card'); card.appendChild(el('div',null,`<b>${CURVE_TITLES[key]||key}</b> ${optLabel(cv.opts||'')}`));
        for(const lb of Object.keys(b)){const x=b[lb]; if(!x||typeof x!=='object')continue; const row=el('div','kv');
          row.innerHTML=`<span class="k">${lb} <span class="muted">(n${x.n||0})</span></span><span class="v">p95 ${x.p95!=null?fmtms(x.p95):'—'} ms</span>`; card.appendChild(row);}
        g.appendChild(card);}
      if(g.children.length)app.appendChild(g);}
  }}
})();

// theme toggle
const btn=document.getElementById('themeBtn');
btn.onclick=()=>{const r=document.documentElement; const cur=r.getAttribute('data-theme'); const next=cur==='dark'?'light':cur==='light'?'':'dark';
  if(next)r.setAttribute('data-theme',next); else r.removeAttribute('data-theme');};
</script>
</body>
</html>"""

if __name__ == "__main__":
    p = build()
    print(f"wrote {p} ({p.stat().st_size//1024} KB)")
