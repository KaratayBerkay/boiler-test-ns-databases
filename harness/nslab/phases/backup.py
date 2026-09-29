"""Phase: backup & restore — take one consistent backup with the engine's own tooling (timed, with a concurrent
read probe to show the impact on live traffic), mutate the dataset, restore the backup (timed, downtime measured
when the engine has to go offline), and prove the restore by fingerprinting the data before and after.

The fingerprint is deliberately cheap and engine-agnostic: counts of the small collections, a set of customer
documents that get deleted after the backup, and a `repl_probe` collection that receives 100 docs before the backup
and 500 more after it. A verified restore has the 100, not the 600, and the customers back.
"""
from __future__ import annotations

import random
import threading
import time
from pathlib import Path
from typing import Any

from .. import dockerctl
from ..config import StackConfig
from ..engines import Engine
from ..engines.base import with_timeout
from ..ops import Unsupported
from ..util import short_err, summarize

PROBE = "repl_probe"
VERIFY_COUNTS = ["customers", "categories", "products", "inventory"]     # cheap COUNTs everywhere (orders/events are slow on Cassandra)
DELETED_CUSTOMERS = list(range(1, 51))
PRE_PROBES = 100
POST_PROBES = 500


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def fingerprint(client, *, log=print) -> dict[str, Any]:
    fp: dict[str, Any] = {"counts": {}, "customers_present": 0, "probe_count": None, "post_probe_present": False}
    for c in VERIFY_COUNTS:
        try:
            fp["counts"][c] = client.count(c)
        except Exception as e:  # noqa: BLE001
            fp["counts"][c] = f"error: {short_err(e)}"
    fp["customers_present"] = sum(1 for i in DELETED_CUSTOMERS if client.get("customers", i))
    try:
        fp["probe_count"] = client.count(PROBE)
    except Exception as e:  # noqa: BLE001
        fp["probe_count"] = f"error: {short_err(e)}"
    fp["post_probe_present"] = bool(client.get(PROBE, PRE_PROBES + 1))
    return fp


class ReadSampler:
    """Point reads on the primary while a backup runs: p50/p95 during vs. a baseline shows the backup's cost."""

    def __init__(self, engine: Engine, n_customers: int = 20000):
        self.engine = engine
        self.n = n_customers
        self.samples: list[float] = []
        self.errors = 0
        self._stop = threading.Event()
        self._th: threading.Thread | None = None

    def _run(self):
        rng = random.Random(7)
        try:
            c = self.engine.connect_primary(timeout=10)
        except Exception:  # noqa: BLE001
            self.errors += 1
            return
        try:
            while not self._stop.is_set():
                t0 = time.perf_counter()
                try:
                    c.get("customers", rng.randint(51, self.n))
                    self.samples.append((time.perf_counter() - t0) * 1000)
                except Exception:  # noqa: BLE001
                    self.errors += 1
                    time.sleep(0.05)
                time.sleep(0.002)
        finally:
            c.close()

    def start(self):
        self._th = threading.Thread(target=self._run, daemon=True)
        self._th.start()
        return self

    def stop(self) -> dict[str, Any]:
        self._stop.set()
        if self._th:
            self._th.join(timeout=10)
        return {"reads": len(self.samples), "errors": self.errors, "latency_ms": summarize(self.samples)}


class Availability:
    """Ping the primary every 100 ms; the longest gap of failed pings is the observed downtime of an offline step."""

    def __init__(self, engine: Engine):
        self.engine = engine
        self._stop = threading.Event()
        self.down_windows: list[float] = []
        self.first_fail: float | None = None
        self._th: threading.Thread | None = None

    def _run(self):
        c = None
        while not self._stop.is_set():
            ok = False
            try:
                if c is None:
                    c = self.engine.connect_primary(timeout=2)
                with_timeout(c.ping, 2, "ping")
                ok = True
            except Exception:  # noqa: BLE001
                try:
                    if c is not None:
                        c.close()
                except Exception:  # noqa: BLE001
                    pass
                c = None
            now = time.perf_counter()
            if not ok and self.first_fail is None:
                self.first_fail = now
            elif ok and self.first_fail is not None:
                self.down_windows.append(now - self.first_fail)
                self.first_fail = None
            time.sleep(0.1)
        if self.first_fail is not None:
            self.down_windows.append(time.perf_counter() - self.first_fail)
        if c is not None:
            c.close()

    def start(self):
        self._th = threading.Thread(target=self._run, daemon=True)
        self._th.start()
        return self

    def stop(self) -> dict[str, Any]:
        self._stop.set()
        if self._th:
            self._th.join(timeout=15)
        return {"downtime_ms": round(max(self.down_windows) * 1000, 1) if self.down_windows else 0.0,
                "outages": len(self.down_windows)}


def export_backup(engine: Engine, cfg: StackConfig, tag: str, dest_root: Path) -> dict[str, Any]:
    """Copy the artifact of `tag` off the stack (docker cp out of the backups volume) to dest_root/<stack>/<tag>/.
    This is the step `down -v` makes mandatory: the backups volume dies with the stack."""
    import shutil
    dest = dest_root / cfg.key / tag
    dest.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    copied = []
    for container, path in engine.artifact_paths(tag):
        target = dest / Path(path).name
        if container == "host":
            src = Path(path)
            if src.is_dir():
                if target.exists():
                    shutil.rmtree(target)
                shutil.copytree(src, target)
            else:
                shutil.copy2(src, target)
        else:
            if target.exists():
                shutil.rmtree(target) if target.is_dir() else target.unlink()
            dockerctl.copy_out(container, path, target)
        copied.append(str(target))
    size = sum(f.stat().st_size for f in dest.rglob("*") if f.is_file())
    return {"dir": str(dest), "files": copied, "bytes": size, "seconds": round(time.perf_counter() - t0, 2)}


def import_backup(engine: Engine, cfg: StackConfig, tag: str, src_root: Path) -> dict[str, Any]:
    """Copy an exported artifact back into a (fresh) stack's backups volume, where restore() expects it."""
    import shutil
    src = src_root / cfg.key / tag
    if not src.exists():
        raise FileNotFoundError(f"no exported backup at {src}")
    t0 = time.perf_counter()
    prepare_backup_dirs(cfg)
    placed = []
    for container, path in engine.artifact_paths(tag):
        item = src / Path(path).name
        if container == "host":
            target = Path(path)
            if target.exists():
                shutil.rmtree(target) if target.is_dir() else target.unlink()
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(item, target) if item.is_dir() else shutil.copy2(item, target)
        else:
            dockerctl.exec_in(container, ["rm", "-rf", path], user="0", timeout=600)
            dockerctl.copy_in(container, item, str(Path(path).parent))
        placed.append(f"{container}:{path}")
    return {"placed": placed, "seconds": round(time.perf_counter() - t0, 2)}


def run_dr_drill(engine: Engine, cfg: StackConfig, scale: float, *, export_root: Path, log=print) -> dict[str, Any]:
    """Disaster-recovery drill: backup -> export off the stack -> destroy the stack (compose down -v / wipe) -> bring a
    fresh stack up -> import -> restore -> verify against the pre-destruction fingerprint."""
    tag = f"nslab-dr-{time.strftime('%Y%m%d-%H%M%S')}"
    out: dict[str, Any] = {"tag": tag}
    prepare_backup_dirs(cfg)
    # a preceding --failover run leaves the old primary restarting: wait until it answers before fingerprinting
    dockerctl.wait_for(lambda: engine.connect_primary(timeout=5).ping(), timeout=300, interval=2.0, desc="primary reachable")
    client = engine.connect_primary(timeout=30)
    try:
        try:
            client.drop_collection(PROBE)
        except Exception:  # noqa: BLE001
            pass
        if hasattr(engine, "create_probe_collection"):
            engine.create_probe_collection(client)
        client.bulk_put(PROBE, [{"id": i, "ts_ms": _now_ms()} for i in range(1, PRE_PROBES + 1)])
        if hasattr(client, "refresh"):
            client.refresh()
        pre = fingerprint(client, log=log)
        out["fingerprint_before"] = pre
        log(f"  fingerprint: counts={pre['counts']} customers 1-50={pre['customers_present']} probe={pre['probe_count']}")
        t0 = time.perf_counter()
        info = engine.backup(client, tag, log=log)
        info["seconds"] = round(time.perf_counter() - t0, 2)
        out["backup"] = {k: v for k, v in info.items() if k != "reads_during"}
        log(f"  backup [{info.get('method')}] {info['seconds']}s artifact {info.get('artifact_bytes') or 0:,} B")
    finally:
        client.close()
    exp = export_backup(engine, cfg, tag, export_root)
    out["export"] = exp
    log(f"  exported {exp['bytes']:,} B to {exp['dir']} in {exp['seconds']}s")
    # --- destroy
    t0 = time.perf_counter()
    if cfg.has_compose:
        dockerctl.down(cfg.stack_dir, cfg.project, volumes=True)
        out["destroy"] = {"method": "docker compose down -v (data + backups volumes removed)", "seconds": round(time.perf_counter() - t0, 1)}
        log(f"  stack destroyed (down -v) in {out['destroy']['seconds']}s; bringing a fresh one up ...")
        t1 = time.perf_counter()
        r = dockerctl.up(cfg.stack_dir, cfg.project, oneshot=cfg.oneshot)
        time.sleep(float(cfg.features.get("settle_seconds", 3)))
        prepare_backup_dirs(cfg)
        out["fresh_up"] = {"seconds": r["seconds"], "services": r["services"]}
        log(f"  fresh stack up in {r['seconds']}s")
    else:
        engine.wipe()
        out["destroy"] = {"method": "embedded: data files deleted", "seconds": round(time.perf_counter() - t0, 2)}
        out["fresh_up"] = {"seconds": 0.0}
        log("  embedded data files deleted")
    # --- prove the data is gone
    gone: dict[str, Any] = {}
    try:
        c = engine.connect_primary(timeout=60)
        try:
            for coll in ("customers", PROBE):
                try:
                    gone[coll] = c.count(coll)
                except Exception as e:  # noqa: BLE001
                    gone[coll] = f"error: {short_err(e)[:80]}"
        finally:
            c.close()
    except Exception as e:  # noqa: BLE001
        gone["connect"] = f"error: {short_err(e)[:80]}"
    out["after_destroy"] = gone
    log(f"  after destroy: {gone}")
    # --- import + restore + verify
    imp = import_backup(engine, cfg, tag, export_root)
    out["import"] = imp
    log(f"  imported artifact back into the fresh stack in {imp['seconds']}s")
    client = engine.connect_primary(timeout=60)
    try:
        t0 = time.perf_counter()
        rinfo = engine.restore(client, tag, info, log=log)
        rinfo["seconds"] = round(time.perf_counter() - t0, 2)
    finally:
        client.close()
    client = engine.connect_primary(timeout=60)
    try:
        if hasattr(client, "refresh"):
            client.refresh()
        post = fingerprint(client, log=log)
        try:
            client.drop_collection(PROBE)
        except Exception:  # noqa: BLE001
            pass
    finally:
        client.close()
    checks = {"counts_match": post["counts"] == pre["counts"], "customers_present": post["customers_present"] == pre["customers_present"],
              "probe_count_match": post["probe_count"] == pre["probe_count"]}
    rinfo["checks"] = checks
    rinfo["verified"] = all(checks.values())
    out["restore"] = rinfo
    out["fingerprint_after_restore"] = post
    log(f"  restore [{rinfo.get('method')}] {rinfo['seconds']}s -> verified={rinfo['verified']} {'' if rinfo['verified'] else checks}")
    return out


def prepare_backup_dirs(cfg: StackConfig) -> None:
    """A fresh named volume is root-owned; the engines run as redis/mongodb/cassandra/neo4j/uid 1000 — open /backups up."""
    for t in cfg.targets:
        if t.container:
            try:
                dockerctl.exec_in(t.container, ["sh", "-c", "mkdir -p /backups && chmod 1777 /backups"], user="0", timeout=30)
            except Exception:  # noqa: BLE001
                pass


def run_backup(engine: Engine, cfg: StackConfig, scale: float, *, log=print) -> dict[str, Any]:
    tag = f"nslab-{time.strftime('%Y%m%d-%H%M%S')}"
    out: dict[str, Any] = {"tag": tag, "strategy": dict(engine.BACKUP_STRATEGY)}
    prepare_backup_dirs(cfg)
    client = engine.connect_primary(timeout=30)
    try:
        try:
            out["data_bytes"] = engine.data_bytes(client)
        except Exception as e:  # noqa: BLE001
            out["data_bytes"] = None
            out["data_bytes_error"] = short_err(e)
        # --- state before the backup: probe collection with PRE_PROBES docs
        try:
            client.drop_collection(PROBE)
        except Exception:  # noqa: BLE001
            pass
        if hasattr(engine, "create_probe_collection"):
            engine.create_probe_collection(client)
        client.bulk_put(PROBE, [{"id": i, "ts_ms": _now_ms()} for i in range(1, PRE_PROBES + 1)])
        if hasattr(client, "refresh"):
            client.refresh()
        pre = fingerprint(client, log=log)
        out["fingerprint_before"] = pre
        log(f"  fingerprint before backup: counts={pre['counts']} customers 1-50 present={pre['customers_present']} probe={pre['probe_count']}")
        # --- baseline point-read latency, then the backup with the sampler running
        base = ReadSampler(engine).start()
        time.sleep(2.0)
        out["reads_baseline"] = base.stop()
        sampler = ReadSampler(engine).start()
        t0 = time.perf_counter()
        try:
            info = engine.backup(client, tag, log=log)
            info["seconds"] = round(time.perf_counter() - t0, 2)
            info["status"] = "ok"
        except Unsupported as e:
            info = {"status": "unsupported", "detail": str(e)[:300], "seconds": round(time.perf_counter() - t0, 2)}
        except Exception as e:  # noqa: BLE001
            info = {"status": "error", "error": short_err(e), "seconds": round(time.perf_counter() - t0, 2)}
        info["reads_during"] = sampler.stop()
        out["backup"] = info
        if info["status"] != "ok":
            log(f"  backup {info['status']}: {info.get('detail') or info.get('error')}")
            return out
        ab, db = info.get("artifact_bytes"), out.get("data_bytes")
        info["artifact_pct_of_data"] = round(100 * ab / db, 1) if ab and db else None
        b_p50 = out["reads_baseline"]["latency_ms"].get("p50")
        d_p50 = info["reads_during"]["latency_ms"].get("p50")
        log(f"  backup [{info.get('method')}] {info['seconds']}s  artifact {ab or 0:,} B ({info['artifact_pct_of_data']}% of data)  "
            f"online={info.get('online')}{'' if info.get('online') else ' downtime ' + str(info.get('downtime_ms')) + ' ms'}  "
            f"point-read p50 during {d_p50 if d_p50 is None else round(d_p50, 2)} ms vs baseline {b_p50 if b_p50 is None else round(b_p50, 2)} ms, "
            f"{info['reads_during']['errors']} read errors")
        if info.get("online") is False:                 # the server restarted: drivers keep dead pooled connections, reconnect
            client.close()
            client = engine.connect_primary(timeout=60)
        # --- mutate: more probe docs + delete customers
        client.bulk_put(PROBE, [{"id": i, "ts_ms": _now_ms()} for i in range(PRE_PROBES + 1, PRE_PROBES + POST_PROBES + 1)])
        for i in DELETED_CUSTOMERS:
            client.delete("customers", i)
        if hasattr(client, "refresh"):
            client.refresh()
        mid = fingerprint(client, log=log)
        out["fingerprint_after_mutation"] = mid
        log(f"  mutated: probe={mid['probe_count']} customers 1-50 present={mid['customers_present']}")
        # --- restore with the availability poller running
        avail = Availability(engine).start()
        t0 = time.perf_counter()
        try:
            rinfo = engine.restore(client, tag, info, log=log)
            rinfo["seconds"] = round(time.perf_counter() - t0, 2)
            rinfo["status"] = "ok"
        except Unsupported as e:
            rinfo = {"status": "unsupported", "detail": str(e)[:300], "seconds": round(time.perf_counter() - t0, 2)}
        except Exception as e:  # noqa: BLE001
            rinfo = {"status": "error", "error": short_err(e), "seconds": round(time.perf_counter() - t0, 2)}
        time.sleep(0.5)
        rinfo["availability"] = avail.stop()
        if rinfo.get("downtime_ms") is not None:          # offline restores measure stop->first successful ping themselves
            rinfo["availability"]["downtime_ms"] = max(rinfo["availability"]["downtime_ms"], rinfo["downtime_ms"])
        out["restore"] = rinfo
        if rinfo["status"] != "ok":
            log(f"  restore {rinfo['status']}: {rinfo.get('detail') or rinfo.get('error')}")
            return out
    finally:
        client.close()
    # --- verify on a fresh connection (containers may have restarted, embedded handles reopened)
    client = engine.connect_primary(timeout=60)
    try:
        if hasattr(client, "refresh"):
            client.refresh()
        post = fingerprint(client, log=log)
        out["fingerprint_after_restore"] = post
        checks = {
            "counts_match": post["counts"] == pre["counts"],
            "deleted_customers_back": post["customers_present"] == len(DELETED_CUSTOMERS),
            "probe_count_back_to_pre": post["probe_count"] == pre["probe_count"] == PRE_PROBES,
            "post_backup_writes_gone": not post["post_probe_present"],
        }
        rinfo["checks"] = checks
        rinfo["verified"] = all(checks.values())
        log(f"  restore [{rinfo.get('method')}] {rinfo['seconds']}s  downtime {rinfo['availability']['downtime_ms']} ms  "
            f"verified={rinfo['verified']} {'' if rinfo['verified'] else checks}")
        # --- incremental second backup (engines with segment/file-level incremental artifacts)
        try:
            client.bulk_put(PROBE, [{"id": i, "ts_ms": _now_ms()} for i in range(PRE_PROBES + 1, PRE_PROBES + 11)])
            if hasattr(client, "refresh"):
                client.refresh()
            t0 = time.perf_counter()
            inc = engine.backup_incremental(client, tag + "-inc", info, log=log)
            inc["seconds"] = round(time.perf_counter() - t0, 2)
            inc["status"] = "ok"
            out["incremental"] = inc
            log(f"  incremental backup [{inc.get('method')}] {inc['seconds']}s  new bytes {inc.get('artifact_bytes_delta')}")
        except Unsupported as e:
            out["incremental"] = {"status": "unsupported", "detail": str(e)[:300]}
        except Exception as e:  # noqa: BLE001
            out["incremental"] = {"status": "error", "error": short_err(e)}
        try:
            client.drop_collection(PROBE)
        except Exception:  # noqa: BLE001
            pass
    finally:
        client.close()
    return out
