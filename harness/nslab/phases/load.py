"""Phase: schema creation + bulk data load."""
from __future__ import annotations

from typing import Any

from ..config import StackConfig
from ..datagen import counts_for, ensure_docs
from ..engines import Engine
from ..timing import Stopwatch
from ..util import short_err


def run_load(engine: Engine, cfg: StackConfig, scale: float, *, log=print) -> dict[str, Any]:
    out: dict[str, Any] = {"scale": scale, "collections": {}, "schema": [], "errors": []}
    paths = ensure_docs(scale)
    expected = counts_for(scale)
    client = engine.connect_primary(timeout=30)
    try:
        sw = Stopwatch()
        engine.reset(client)
        out["schema"] = engine.create_schema(client)
        out["schema_seconds"] = round(sw.ms() / 1000, 2)
        log(f"  schema created ({len(out['schema'])} items, {out['schema_seconds']}s)")
        total = Stopwatch()
        for name in engine.collections():
            sw = Stopwatch()
            try:
                n, method = engine.load_collection(client, name, paths[name], expected.get(name, 0))
                secs = sw.ms() / 1000
                try:
                    actual = client.count(name)
                except Exception as e:  # noqa: BLE001
                    actual = f"count failed: {short_err(e)}"
                out["collections"][name] = {"rows": n, "counted": actual, "expected": expected.get(name), "seconds": round(secs, 2),
                                            "rows_per_s": round(n / secs) if secs > 0 else None, "method": method,
                                            "ok": actual == expected.get(name, n)}
                log(f"  {name:12s} {n:>9,} docs  {secs:7.2f}s  {out['collections'][name]['rows_per_s'] or 0:>9,} docs/s  [{method}]"
                    + ("" if out["collections"][name]["ok"] else f"  (counted {actual})"))
            except Exception as e:  # noqa: BLE001
                out["collections"][name] = {"error": short_err(e), "seconds": round(sw.ms() / 1000, 2)}
                out["errors"].append(f"{name}: {short_err(e)}")
                log(f"  {name:12s} FAILED: {short_err(e)}")
        out["load_seconds"] = round(total.ms() / 1000, 2)
        sw = Stopwatch()
        try:
            out["after_load"] = engine.after_load(client)
        except Exception as e:  # noqa: BLE001
            out["after_load"] = [f"FAILED: {short_err(e)}"]
        out["post_seconds"] = round(sw.ms() / 1000, 2)
        out["server_version"] = client.server_version()
    finally:
        client.close()
    return out
