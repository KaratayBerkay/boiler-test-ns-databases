"""nslab command line."""
from __future__ import annotations

import json
import time

import typer
from rich.console import Console

from . import dockerctl, runlog
from .config import list_stacks, load_stack
from .datagen import counts_for, ensure_docs
from .engines import make_engine
from .report import build_summary, save_result
from .util import now_iso, run_id as new_run_id, short_err

app = typer.Typer(help="NoSQL database lab: capability, optimisation, connection, replication, backup/restore and logging tests across "
                       "document, key-value, wide-column, graph, search, time-series and vector engines.", no_args_is_help=True)
con = Console()


def log(msg: str) -> None:
    con.print(msg, highlight=False, markup=False)
    runlog.info(msg)


runlog.setup()


@app.command("list")
def cmd_list():
    """List configured stacks."""
    for k in list_stacks():
        c = load_stack(k)
        log(f"{k:16s} {c.display:34s} family={c.family:10s} driver={c.driver:10s} targets={[t.name for t in c.targets]} compose={'yes' if c.has_compose else 'no'}")


@app.command()
def gen(scale: float = typer.Option(1.0, help="data scale factor"), force: bool = False):
    """Generate the deterministic NDJSON dataset."""
    t0 = time.time()
    paths = ensure_docs(scale, force=force)
    log(f"generated in {time.time()-t0:.1f}s: {counts_for(scale)} -> {paths['events'].parent}")


@app.command()
def up(stack: str, pull: bool = False):
    """Start a stack and wait until healthy."""
    c = load_stack(stack)
    if not c.has_compose:
        log("embedded engine: nothing to start")
        return
    r = dockerctl.up(c.stack_dir, c.project, oneshot=c.oneshot, pull=pull)
    from .phases.backup import prepare_backup_dirs
    prepare_backup_dirs(c)
    log(f"up: {r}")


@app.command()
def down(stack: str, volumes: bool = True):
    """Stop a stack (and remove its volumes)."""
    c = load_stack(stack)
    if c.has_compose:
        dockerctl.down(c.stack_dir, c.project, volumes=volumes)
    log("down")


def _phase(engine, cfg, name: str, fn, result: dict, **kw):
    runlog.context(run_id=result.get("run_id"), stack=cfg.key, phase=name)
    runlog.event("phase_start")
    log(f"[{cfg.key}] phase: {name}")
    t0 = time.time()
    try:
        result["phases"][name] = fn(engine, cfg, **kw)
        if isinstance(result["phases"][name], dict):
            result["phases"][name]["_seconds"] = round(time.time() - t0, 1)
        runlog.event("phase_end", seconds=round(time.time() - t0, 1))
    except Exception as e:  # noqa: BLE001
        result["phases"][name] = {"_error": short_err(e), "_seconds": round(time.time() - t0, 1)}
        log(f"  phase {name} FAILED: {short_err(e)}")
        runlog.event("phase_failed", seconds=round(time.time() - t0, 1), error=short_err(e))
    finally:
        runlog.context(phase=None)
    save_result(cfg.key, result["run_id"], result)


# backup/logging run before replication: `--failover` leaves the old primary demoted, and a restore must target the primary
ALL_PHASES = ["load", "capabilities", "bench", "optimize", "connections", "loadtest", "backup", "logging", "replication"]


def _run_phases(stack: str, phases: list[str], scale: float | None, failover: bool, storms: list[int] | None = None, only: str | None = None) -> dict:
    cfg = load_stack(stack)
    scale = scale if scale is not None else cfg.scale
    engine = make_engine(cfg)
    result = {"engine": cfg.key, "display": cfg.display, "category": cfg.category, "family": cfg.family, "driver": cfg.driver,
              "image": cfg.image, "run_id": new_run_id(), "started": now_iso(), "scale": scale, "notes": cfg.notes, "phases": {}}
    from .phases.load import run_load
    from .phases.capabilities import run_capabilities
    from .phases.bench import run_bench
    from .phases.optimize import run_optimize
    from .phases.connections import run_connections
    from .phases.loadtest import run_loadtest
    from .phases.replication import run_replication
    from .phases.backup import run_backup
    from .phases.logs import run_logging
    ensure_docs(scale)
    only_set = {x.strip() for x in only.split(",")} if only else None
    result["partial"] = bool(only_set)
    if "load" in phases:
        _phase(engine, cfg, "load", lambda e, c: run_load(e, c, scale, log=log), result)
    if "capabilities" in phases:
        _phase(engine, cfg, "capabilities", lambda e, c: run_capabilities(e, c, log=log), result)
    if "bench" in phases:
        _phase(engine, cfg, "bench", lambda e, c: run_bench(e, c, scale, only=only_set, log=log), result)
    if "optimize" in phases:
        _phase(engine, cfg, "optimize", lambda e, c: run_optimize(e, c, scale, log=log), result)
    if "connections" in phases:
        _phase(engine, cfg, "connections", lambda e, c: run_connections(e, c, scale, log=log, storms=storms), result)
    if "loadtest" in phases:
        _phase(engine, cfg, "loadtest", lambda e, c: run_loadtest(e, c, scale, log=log), result)
    if "backup" in phases:
        _phase(engine, cfg, "backup", lambda e, c: run_backup(e, c, scale, log=log), result)
    if "logging" in phases:
        _phase(engine, cfg, "logging", lambda e, c: run_logging(e, c, scale, log=log), result)
    if "replication" in phases:
        _phase(engine, cfg, "replication", lambda e, c: run_replication(e, c, scale, do_failover=failover, log=log), result)
    try:
        c = engine.connect_primary()
        result["server_version"] = c.server_version()
        c.close()
    except Exception:  # noqa: BLE001
        pass
    result["finished"] = now_iso()
    p = save_result(cfg.key, result["run_id"], result)
    log(f"saved {p}")
    return result


@app.command()
def run(stack: str, scale: float = typer.Option(None), phases: str = typer.Option(",".join(ALL_PHASES), help="comma-separated phases"),
        failover: bool = typer.Option(False, help="run the destructive failover test at the end of the replication phase"),
        no_up: bool = typer.Option(False, "--no-up", help="assume the stack is already running"),
        keep: bool = typer.Option(False, help="leave the stack running afterwards"), pull: bool = False,
        storms: str = typer.Option(None, help="comma-separated connection storm sizes"),
        only: str = typer.Option(None, help="comma-separated operation ids for the bench phase")):
    """Bring a stack up, run the selected phases, tear it down."""
    cfg = load_stack(stack)
    ph = [p.strip() for p in phases.split(",") if p.strip()]
    st = [int(x) for x in storms.split(",")] if storms else None
    if cfg.has_compose and not no_up:
        log(f"[{stack}] starting stack ...")
        t0 = time.time()
        r = dockerctl.up(cfg.stack_dir, cfg.project, oneshot=cfg.oneshot, pull=pull)
        from .phases.backup import prepare_backup_dirs
        prepare_backup_dirs(cfg)
        log(f"[{stack}] up in {r['seconds']}s: {r['services']}")
        time.sleep(float(cfg.features.get("settle_seconds", 3)))
    try:
        _run_phases(stack, ph, scale, failover, st, only)
    finally:
        if cfg.has_compose and not keep and not no_up:
            log(f"[{stack}] tearing down ...")
            dockerctl.down(cfg.stack_dir, cfg.project)
    build_summary()


@app.command()
def phase(stack: str, name: str, scale: float = typer.Option(None), failover: bool = False, storms: str = typer.Option(None),
          only: str = typer.Option(None, help="comma-separated operation ids (bench only)")):
    """Run a single phase against an already-running stack."""
    st = [int(x) for x in storms.split(",")] if storms else None
    _run_phases(stack, [name], scale, failover, st, only)
    build_summary()


@app.command()
def report():
    """Rebuild results/SUMMARY.md from all latest.json files."""
    md, js = build_summary()
    log(f"wrote {md} and {js}")


@app.command()
def op(stack: str, op_id: str, params: str = typer.Option("{}", help="JSON parameters"), target: str = "primary"):
    """Run one catalog operation ad hoc against a running stack and print the rows."""
    cfg = load_stack(stack)
    eng = make_engine(cfg)
    t = cfg.by_name(target) or cfg.primary
    c = eng.connect(t)
    try:
        t0 = time.perf_counter()
        rows = getattr(c, f"op_{op_id}")(json.loads(params))
        ms = (time.perf_counter() - t0) * 1000
        for r in (rows or [])[:50]:
            log(str(r))
        log(f"({len(rows) if rows else 0} rows, {ms:.2f} ms)")
    finally:
        c.close()


@app.command()
def info(stack: str, target: str = "primary"):
    """Version, role and replication status of a target."""
    cfg = load_stack(stack)
    eng = make_engine(cfg)
    t = cfg.by_name(target) or cfg.primary
    c = eng.connect(t)
    try:
        log(f"version: {c.server_version()}")
        log(f"role: {c.role()}")
        log(f"replication: {eng.replication_status(c)}")
        log(f"max_connections: {eng.max_connections(c)}")
    finally:
        c.close()


BACKUP_EXPORT_ROOT = "data/backups"      # off-stack copies of artifacts: <root>/<stack>/<tag>/ (survives `down -v`)


@app.command()
def backup(stack: str, tag: str = typer.Option(None, help="artifact name (default: nslab-<timestamp>)"),
           restore: bool = typer.Option(False, help="restore the given --tag instead of taking a backup"),
           export: bool = typer.Option(True, help="copy the artifact off the stack into --dir after backing up"),
           import_: bool = typer.Option(False, "--import", help="before restoring, copy the exported artifact from --dir back into the stack"),
           dir: str = typer.Option(BACKUP_EXPORT_ROOT, help="host directory for exported artifacts")):
    """Take a backup with the engine's own tooling and export it off the stack (or import + restore one) — the strategy the backup phase measures."""
    from pathlib import Path
    from .phases.backup import prepare_backup_dirs, export_backup, import_backup
    from .util import ROOT
    cfg = load_stack(stack)
    eng = make_engine(cfg)
    root = Path(dir) if Path(dir).is_absolute() else ROOT / dir
    prepare_backup_dirs(cfg)
    if restore and not tag:
        raise typer.BadParameter("--restore needs --tag")
    if restore and import_:
        log(f"import: {import_backup(eng, cfg, tag, root)}")
    c = eng.connect_primary(timeout=30)
    try:
        t0 = time.time()
        if restore:
            r = eng.restore(c, tag, {}, log=log)
        else:
            tag = tag or f"nslab-{time.strftime('%Y%m%d-%H%M%S')}"
            r = eng.backup(c, tag, log=log)
        r["seconds"] = round(time.time() - t0, 2)
        for k, v in r.items():
            log(f"{k:18s} {v}")
    finally:
        c.close()
    if not restore and export:
        e = export_backup(eng, cfg, tag, root)
        log(f"exported {e['bytes']:,} B -> {e['dir']} ({e['seconds']}s)")
    log(f"tag: {tag}")


@app.command()
def dr(stack: str, dir: str = typer.Option(BACKUP_EXPORT_ROOT, help="host directory for the exported artifact")):
    """Disaster-recovery drill on a loaded, running stack: backup -> export -> `down -v` -> fresh `up` -> import -> restore -> verify."""
    from pathlib import Path
    from .phases.backup import run_dr_drill
    from .util import ROOT
    cfg = load_stack(stack)
    eng = make_engine(cfg)
    root = Path(dir) if Path(dir).is_absolute() else ROOT / dir
    runlog.context(run_id=new_run_id(), stack=stack, phase="dr")
    t0 = time.time()
    try:
        r = run_dr_drill(eng, cfg, cfg.scale, export_root=root, log=log)
        r["_seconds"] = round(time.time() - t0, 1)
    except Exception as e:  # noqa: BLE001
        r = {"_error": short_err(e), "_seconds": round(time.time() - t0, 1)}
        log(f"  dr drill FAILED: {short_err(e)}")
    save_result(cfg.key, runlog._CTX["run_id"], {"engine": cfg.key, "run_id": runlog._CTX["run_id"], "partial": True, "phases": {"backup": {"dr": r}}})
    build_summary()


@app.command()
def slowlog(stack: str, threshold_ms: int = typer.Option(100, help="entries slower than this"),
            exercise: bool = typer.Option(False, help="also run the adapter's known-slow operation first")):
    """Read the engine's slow-query log through its own interface (SLOWLOG, system.profile, debug.log, query collector, index slowlog)."""
    cfg = load_stack(stack)
    eng = make_engine(cfg)
    c = eng.connect_primary(timeout=30)
    try:
        if exercise:
            st = eng.slow_query_setup(c, threshold_ms)
            if isinstance(st, tuple):
                st, threshold_ms = st[0], int(st[1])
            log(f"setup: {st}")
            log(f"slow op: {eng.slow_query_slow_op(c)}")
            time.sleep(float(cfg.features.get("slowlog_settle_seconds", 0.5)))
        for e in eng.slow_query_entries(c, threshold_ms):
            log(f"{str(e.get('ms')):>10s} ms  {e.get('text')}")
        if exercise:
            eng.slow_query_teardown(c)
    finally:
        c.close()


# --- scenarios (domain workloads beyond the generic operation catalog) ------------------------
scenario = typer.Typer(help="Domain-shaped workloads (currently: 'messaging' — a social chat store).", no_args_is_help=True)
app.add_typer(scenario, name="scenario")


def _save_scenario(stack: str, name: str, obj: dict):
    from .util import dump_json, results_root
    from . import platform
    obj["run_id"] = new_run_id()
    obj["saved"] = now_iso()
    obj["platform"] = "k3s" if platform.k8s() else "compose"
    p = results_root() / stack / f"scenario-{name}-{obj['run_id']}.json"
    dump_json(p, obj)
    dump_json(results_root() / stack / f"scenario-{name}-latest.json", obj)
    return p


def _opts(fanout: str) -> dict:
    return {"fanout": fanout}


@scenario.command("list")
def scenario_list():
    """List available scenarios."""
    from .scenarios import base
    for n in base.names():
        log(f"{n:14s} {base.get(n).blurb}")


@scenario.command("seed")
def scenario_seed(stack: str, scenario_name: str = typer.Option("messaging", "--scenario", "-s"),
                  size: str = typer.Option("small", help="smoke|small|full"), workers: int = 8):
    """Create the scenario schema and load its seeded corpus."""
    from .scenarios import base
    scen = base.get(scenario_name)
    r = scen.seed(stack, size, workers=workers, log=log)
    log(f"saved {_save_scenario(stack, scenario_name + '-seed', r)}")


@scenario.command("run")
def scenario_run(stack: str, scenario_name: str = typer.Option("messaging", "--scenario", "-s"),
                 size: str = typer.Option("small"), seconds: float = typer.Option(20.0),
                 workers: str = typer.Option("8,32,64", help="comma-separated worker-count ramp"),
                 fanout: str = typer.Option("write", help="messaging: write (fan-out-on-write) | read (fan-in-on-read)"),
                 mix: str = typer.Option(None, help="comma-separated op names to restrict the mix"),
                 rate: float = typer.Option(None, help="open-loop target ops/s (default: closed-loop)"),
                 think_ms: float = typer.Option(0.0, help="closed-loop per-op think time (ms)")):
    """Run the steady-state mixed load, ramping worker counts, and record per-op percentiles + throughput."""
    from .scenarios import driver
    wl = [int(x) for x in workers.split(",") if x.strip()]
    mn = [x.strip() for x in mix.split(",")] if mix else None
    r = driver.run(scenario_name, stack, size, workers_list=wl, seconds=seconds, opts=_opts(fanout), mix_names=mn, rate=rate, think_ms=think_ms, log=log)
    log(f"saved {_save_scenario(stack, scenario_name, r)}")


@scenario.command("curves")
def scenario_curves(stack: str, scenario_name: str = typer.Option("messaging", "--scenario", "-s"),
                    size: str = typer.Option("small"), fanout: str = typer.Option("write"), samples: int = 400):
    """Measure the scenario's analysis curves (scenario-specific)."""
    from .scenarios import base
    scen = base.get(scenario_name)
    r = scen.curves(stack, size, _opts(fanout), samples=samples, log=log)
    log(f"saved {_save_scenario(stack, scenario_name + '-curves', r)}")


@scenario.command("drop")
def scenario_drop(stack: str, scenario_name: str = typer.Option("messaging", "--scenario", "-s")):
    """Drop the scenario schema."""
    from .config import load_stack as _ls
    from .scenarios import base
    be = base.get(scenario_name).make_backend(stack, _ls(stack), {}); be.connect(); be.drop_schema(); be.close()
    log("dropped")


if __name__ == "__main__":
    app()
