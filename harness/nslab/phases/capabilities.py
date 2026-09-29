"""Phase: capability probes (features that the timed operations do not exercise)."""
from __future__ import annotations

import time
from typing import Any

from ..config import StackConfig
from ..engines import Engine
from ..ops import Unsupported
from ..probes import PROBES
from ..util import short_err


def run_capabilities(engine: Engine, cfg: StackConfig, *, log=print) -> dict[str, Any]:
    results: dict[str, Any] = {}
    client = engine.connect_primary(timeout=30)
    try:
        static = getattr(client, "STATIC_PROBES", {}) or {}
        for p in PROBES:
            rec: dict[str, Any] = {"title": p.title, "group": p.group}
            t0 = time.perf_counter()
            fn = getattr(client, f"probe_{p.name}", None)
            if fn is None and p.name in static:
                rec["status"], rec["detail"] = static[p.name]
                rec["source"] = "declared"
            elif fn is None:
                rec["status"] = "unsupported"
                rec["detail"] = "no implementation for this engine family"
            else:
                try:
                    detail = fn()
                    rec["status"] = "supported"
                    rec["detail"] = str(detail)[:300] if detail is not None else ""
                except Unsupported as e:
                    rec["status"] = "unsupported"
                    rec["detail"] = str(e)[:300]
                except NotImplementedError as e:
                    rec["status"] = "unsupported"
                    rec["detail"] = str(e)[:300]
                except Exception as e:  # noqa: BLE001
                    rec["status"] = "unsupported" if engine.is_unsupported_error(e) else "error"
                    rec["error"] = short_err(e)
                    rec["error_code"] = engine.error_code(e)
            rec["ms"] = round((time.perf_counter() - t0) * 1000, 1)
            results[p.name] = rec
            mark = {"supported": "OK ", "unsupported": "-- ", "error": "ERR"}[rec["status"]]
            log(f"  {mark} {p.name:24s} {rec.get('error', rec.get('detail', ''))[:100]}")
    finally:
        client.close()
    return results
