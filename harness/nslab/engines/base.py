"""Engine adapter base classes.

`Engine` (one per stack) knows how to open `Client`s against the stack's targets and how to create the
schema / bulk-load the dataset. `Client` (one connection or session) implements the operation catalog as
`op_<id>(params)` methods, the capability probes as `probe_<name>()` methods, and a few generic primitives
(get/put/delete/bulk_put/count) that the replication, connection and load-test phases use.
"""
from __future__ import annotations

import datetime as dt
import time
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

from ..config import StackConfig, Target
from ..datagen import read_docs
from ..ops import CATALOG, Unsupported
from ..schema import COLLECTION_BY_NAME, Collection


class EngineError(RuntimeError):
    pass


def iso(d: Any) -> Any:
    """datetime -> ISO-8601 'Z' string (what NDJSON and most HTTP engines use)."""
    if isinstance(d, dt.datetime):
        return d.strftime("%Y-%m-%dT%H:%M:%SZ")
    return d


def epoch_ms(d: dt.datetime) -> int:
    return int(d.replace(tzinfo=dt.timezone.utc).timestamp() * 1000)


def month_key(d: dt.datetime | str) -> str:
    if isinstance(d, str):
        return d[:7]
    return d.strftime("%Y-%m")


def flatten(doc: dict, sep: str = ".") -> dict:
    """{'attrs': {'lang': 'tr'}} -> {'attrs.lang': 'tr'} (for flat key-value / column stores)."""
    out: dict[str, Any] = {}
    for k, v in doc.items():
        if isinstance(v, dict):
            for kk, vv in flatten(v, sep).items():
                out[f"{k}{sep}{kk}"] = vv
        else:
            out[k] = v
    return out


def batched(it: Iterable, n: int) -> Iterator[list]:
    buf: list = []
    for x in it:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf


class Client:
    """One connection/session to one target. Adapters subclass and implement op_* / probe_* methods."""

    def __init__(self, engine: "Engine", target: Target, raw: Any = None):
        self.engine = engine
        self.target = target
        self.raw = raw
        self.cfg = engine.cfg

    # --- lifecycle / introspection --------------------------------------------------
    def close(self) -> None:
        try:
            if self.raw is not None and hasattr(self.raw, "close"):
                self.raw.close()
        except Exception:  # noqa: BLE001
            pass

    def ping(self) -> Any:
        raise NotImplementedError

    def server_version(self) -> str:
        return "unknown"

    def role(self) -> str:
        """Role as reported by the server (primary/replica/node/unknown)."""
        return self.target.role

    # --- generic primitives used by the phases (replication probes, load test, cleanup) --------------
    def count(self, coll: str) -> int:
        raise NotImplementedError

    def get(self, coll: str, key: Any) -> dict | None:
        raise NotImplementedError

    def put(self, coll: str, doc: dict) -> None:
        raise NotImplementedError

    def delete(self, coll: str, key: Any) -> None:
        raise NotImplementedError

    def bulk_put(self, coll: str, docs: list[dict]) -> int:
        """Fastest batch write the engine offers (bulk API / pipeline / batch). Default: N puts."""
        for d in docs:
            self.put(coll, d)
        return len(docs)

    def load_bulk_put(self, coll: str, docs: list[dict]) -> int:
        """Batch write used only by the load phase (adapters may relax durability here, e.g. w=1)."""
        return self.bulk_put(coll, docs)

    def delete_many(self, coll: str, keys: list[Any]) -> None:
        for k in keys:
            try:
                self.delete(coll, k)
            except Exception:  # noqa: BLE001
                pass

    def drop_collection(self, coll: str) -> None:
        raise NotImplementedError

    # --- helpers for op implementations -------------------------------------------
    @staticmethod
    def rows(docs: Iterable[dict], fields: tuple[str, ...]) -> list[tuple]:
        return [tuple(d.get(f) for f in fields) for d in docs]

    def op_ping(self, p: dict) -> list[tuple]:
        self.ping()
        return [(1,)]

    # standard cleanup hooks referenced by ops.py
    def cleanup_events_by_params(self, params: list[dict]) -> None:
        self.delete_many("events", [p["id"] for p in params])

    def cleanup_batch_events(self, params: list[dict]) -> None:
        self.delete_many("events", [d["id"] for p in params for d in p["docs"]])

    def setup_seed_events(self, params: list[dict]) -> None:
        self.bulk_put("events", [dict(p) for p in params])

    # --- capability probe helpers -------------------------------------------------
    STATIC_PROBES: dict[str, tuple[str, str]] = {}


class Engine:
    driver = "base"
    embedded = False                    # in-process engine: no containers, thread workers instead of processes
    supports_processes = True           # False when connections cannot be forked (embedded single-writer)
    default_batch = 1000

    def __init__(self, cfg: StackConfig):
        self.cfg = cfg
        self.features = cfg.features
        self.apply_features(cfg.features)

    def apply_features(self, features: dict) -> None:
        pass

    # --- connections -----------------------------------------------------------------
    def connect(self, target: Target, *, timeout: float = 10.0) -> Client:
        raise NotImplementedError

    def connect_primary(self, **kw) -> Client:
        return self.connect(self.cfg.primary, **kw)

    # --- schema / data ---------------------------------------------------------------
    def collections(self) -> list[str]:
        """Collections this engine loads (adapters that embed order items skip the flat table)."""
        from ..schema import LOAD_ORDER
        return list(LOAD_ORDER)

    def reset(self, client: Client) -> None:
        for c in self.collections() + ["repl_probe", "scratch"]:
            try:
                client.drop_collection(c)
            except Exception:  # noqa: BLE001
                pass

    def create_schema(self, client: Client) -> list[str]:
        """Create collections / tables / indexes needed by the baseline workload. Returns descriptions."""
        return []

    load_workers = 1                    # >1: fork N processes, each loading every N-th document over its own connection
    load_batch = 1000
    load_parse_dates = True
    load_method = "bulk_put"

    def load_collection(self, client: Client, coll: str, path: Path, expected: int) -> tuple[int, str]:
        """Bulk load one collection from its NDJSON file. Returns (rows, method)."""
        workers = self.load_workers if expected >= 20 * self.load_batch else 1
        if workers <= 1:
            n = 0
            for chunk in batched(read_docs(path, coll, parse_dates=self.load_parse_dates), self.load_batch):
                n += client.load_bulk_put(coll, chunk)
            return n, self.load_method
        return parallel_load(self, coll, path, workers), f"{self.load_method} x{workers} connections"


    def after_load(self, client: Client) -> list[str]:
        """Index builds, refreshes, statistics. Returns descriptions."""
        return []

    # --- optimisation hooks (phases/optimize.py) ---------------------------------------
    def create_index(self, client: Client, name: str, coll: str, fields: list[str]) -> str:
        raise Unsupported(f"{self.driver}: no secondary indexes")

    def drop_index(self, client: Client, name: str, coll: str) -> None:
        pass

    # --- introspection -----------------------------------------------------------------
    def max_connections(self, client: Client) -> int | None:
        return None

    def replication_status(self, client: Client) -> dict[str, Any]:
        return {}

    def promote(self, target: Target) -> str:
        raise EngineError(f"{self.driver}: promote not implemented")

    # --- backup / restore (phases/backup.py) ------------------------------------------------------
    # Static facts rendered next to the measured numbers: tool, consistency model of the artifact, incremental
    # option, point-in-time recovery, how to schedule, how to expire.
    BACKUP_STRATEGY: dict[str, str] = {}

    def data_bytes(self, client: Client) -> int | None:
        """Size of the live data directory (what the backup has to cover)."""
        return None

    def backup(self, client: Client, tag: str, *, log=print) -> dict[str, Any]:
        """Produce one consistent backup artifact named `tag` (online where the engine allows it). Returns
        {"method", "online", "artifact", "artifact_bytes", ...}; raise Unsupported when the engine has no way."""
        raise Unsupported(f"{self.driver}: no backup method implemented")

    def restore(self, client: Client, tag: str, info: dict[str, Any], *, log=print) -> dict[str, Any]:
        """Bring the dataset back to the state captured by `tag`, replacing everything written since. May stop and
        start containers; the phase reconnects afterwards. Returns {"method", "online", ...}."""
        raise Unsupported(f"{self.driver}: no restore method implemented")

    def artifact_paths(self, tag: str) -> list[tuple[str, str]]:
        """Where the artifact of `tag` lives, as (container, path) pairs — or ("host", path) for embedded engines —
        so it can be copied off the stack (docker cp) before `down -v` and copied back into a fresh stack."""
        return [(self.cfg.primary.container or "host", f"/backups/{tag}")]

    def wipe(self) -> None:
        """Destroy the live data of an embedded engine (the `down -v` equivalent); containers use compose down -v."""
        raise Unsupported(f"{self.driver}: wipe is only for embedded engines")

    def backup_incremental(self, client: Client, tag: str, base: dict[str, Any], *, log=print) -> dict[str, Any]:
        """A second backup after a small change, to show what an incremental artifact costs (ES segment-level
        snapshots, Cassandra incremental_backups). Raise Unsupported when the engine only does full copies."""
        raise Unsupported(f"{self.driver}: no incremental backup")

    # --- logging (phases/logs.py) ------------------------------------------------------------------
    LOGGING_STRATEGY: dict[str, str] = {}

    def server_log_facts(self, client: Client) -> dict[str, Any]:
        """Where the engine's own log lives and what it looks like (format, location, how to read it)."""
        return {}

    def slow_query_setup(self, client: Client, threshold_ms: int) -> str:
        """Enable/lower the slow-query log threshold; return a description of what was set."""
        raise Unsupported(f"{self.driver}: no slow-query log")

    def slow_query_fast_op(self, client: Client) -> str:
        """A point operation that must NOT show up in the slow log."""
        return ""

    def slow_query_slow_op(self, client: Client) -> str:
        """An operation known to exceed the threshold (a scan, an unindexed aggregation, a KEYS)."""
        raise Unsupported(f"{self.driver}: no slow-query log")

    def slow_query_entries(self, client: Client, threshold_ms: int) -> list[dict[str, Any]]:
        """Read back the slow-log entries (newest first): [{"ms", "text", ...}]."""
        raise Unsupported(f"{self.driver}: no slow-query log")

    def slow_query_teardown(self, client: Client) -> None:
        pass

    def audit_log_setup(self, client: Client) -> str:
        raise Unsupported(f"{self.driver}: no audit log")

    def audit_log_exercise(self, client: Client) -> list[str]:
        """Statements / events that must appear in the audit log; returns their descriptions."""
        return []

    def audit_log_entries(self, client: Client) -> list[str]:
        raise Unsupported(f"{self.driver}: no audit log")

    def audit_log_teardown(self, client: Client) -> None:
        pass

    def log_level_runtime(self, client: Client) -> str:
        """Raise the server log verbosity at run time (no restart), verify, and put it back."""
        raise Unsupported(f"{self.driver}: log level fixed at start-up")

    # --- error classification ----------------------------------------------------------
    UNSUPPORTED_HINTS = ("not supported", "unsupported", "unimplemented", "not implemented", "unknown command",
                         "no such command", "err unknown", "not available", "unrecognized", "syntax error",
                         "invalid syntax", "not allowed", "feature not", "cannot be used", "unknown operator",
                         "unrecognized pipeline", "no such", "not found for", "parse error", "parsingexception",
                         "unknown function", "invalidrequest", "operation not permitted", "is not enabled",
                         "unknown field", "undefined function", "not a valid")

    def is_unsupported_error(self, exc: BaseException) -> bool:
        if isinstance(exc, (Unsupported, NotImplementedError)):
            return True
        msg = str(exc).lower()
        return any(h in msg for h in self.UNSUPPORTED_HINTS)

    def error_code(self, exc: BaseException) -> str | None:
        for attr in ("code", "status_code", "error_code", "status"):
            v = getattr(exc, attr, None)
            if v and not callable(v) and isinstance(v, (int, str)):
                return str(v)
        args = getattr(exc, "args", ())
        if args and isinstance(args[0], int):
            return str(args[0])
        return None


def parallel_load(engine: "Engine", coll: str, path: Path, workers: int) -> int:
    import multiprocessing as mp

    def work(w: int, q):
        c = engine.connect(engine.cfg.primary, timeout=30)
        n = 0
        buf: list = []
        try:
            for i, doc in enumerate(read_docs(path, coll, parse_dates=engine.load_parse_dates)):
                if i % workers != w:
                    continue
                buf.append(doc)
                if len(buf) >= engine.load_batch:
                    n += c.load_bulk_put(coll, buf)
                    buf = []
            if buf:
                n += c.load_bulk_put(coll, buf)
            q.put((w, n, None))
        except Exception as e:  # noqa: BLE001
            q.put((w, n, f"{type(e).__name__}: {e}"[:300]))
        finally:
            c.close()

    ctx = mp.get_context("fork")
    q = ctx.Queue()
    procs = [ctx.Process(target=work, args=(w, q), daemon=True) for w in range(workers)]
    for p in procs:
        p.start()
    total, errors = 0, []
    for _ in procs:
        w, n, err = q.get(timeout=3600)
        total += n
        if err:
            errors.append(err)
    for p in procs:
        p.join(timeout=10)
    if errors:
        raise EngineError(f"parallel load: {errors[0]}")
    return total


def looks_unsupported(msg: str | None) -> bool:
    if not msg:
        return False
    m = msg.lower()
    return any(h in m for h in Engine.UNSUPPORTED_HINTS)


def with_timeout(fn: Callable[[], Any], seconds: float, what: str) -> Any:
    """Run fn() in a daemon thread; raise TimeoutError if it does not return in time (drivers hang on dead nodes)."""
    import threading
    box: dict[str, Any] = {}

    def run():
        try:
            box["value"] = fn()
        except BaseException as e:  # noqa: BLE001
            box["error"] = e

    th = threading.Thread(target=run, daemon=True)
    th.start()
    th.join(timeout=seconds)
    if th.is_alive():
        raise TimeoutError(f"{what} did not return within {seconds}s")
    if "error" in box:
        raise box["error"]
    return box.get("value")
