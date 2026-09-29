"""Harness-side logging strategy: everything `nslab` prints also lands in a structured, rotated JSONL log.

results/logs/nslab.jsonl  — one JSON object per line: {"ts", "level", "run_id", "stack", "phase", "msg", ...}
Rotates at 20 MB, keeps 5 files (stdlib RotatingFileHandler). Phase start/end/failure events carry `event` +
`seconds`, so a run can be reconstructed (or shipped to Loki/ELK) without parsing console output.
`NSLAB_LOG_LEVEL=DEBUG` raises the level; `NSLAB_LOG_FILE=/path` moves the file.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import logging.handlers
import os
from pathlib import Path
from typing import Any

from .util import RESULTS

_CTX: dict[str, Any] = {"run_id": None, "stack": None, "phase": None}
_LOG = logging.getLogger("nslab")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        d: dict[str, Any] = {"ts": dt.datetime.fromtimestamp(record.created, dt.timezone.utc).isoformat(timespec="milliseconds"),
                             "level": record.levelname, **{k: v for k, v in _CTX.items() if v}, "msg": record.getMessage()}
        extra = getattr(record, "extra", None)
        if isinstance(extra, dict):
            d.update(extra)
        if record.exc_info:
            d["exc"] = self.formatException(record.exc_info)[-2000:]
        return json.dumps(d, default=str)


def setup() -> Path:
    """Idempotent: attach the rotating JSONL handler once per process."""
    path = Path(os.environ.get("NSLAB_LOG_FILE") or (RESULTS / "logs" / "nslab.jsonl"))
    if getattr(setup, "_done", None) == path:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    h = logging.handlers.RotatingFileHandler(path, maxBytes=20 * 1024 * 1024, backupCount=5, encoding="utf-8")
    h.setFormatter(JsonFormatter())
    _LOG.addHandler(h)
    _LOG.setLevel(os.environ.get("NSLAB_LOG_LEVEL", "INFO").upper())
    _LOG.propagate = False
    setup._done = path  # type: ignore[attr-defined]
    return path


def context(**kw: Any) -> None:
    _CTX.update(kw)


def info(msg: str, **extra: Any) -> None:
    _LOG.info(msg, extra={"extra": extra} if extra else None)


def event(name: str, **extra: Any) -> None:
    _LOG.info(name, extra={"extra": {"event": name, **extra}})


def error(msg: str, **extra: Any) -> None:
    _LOG.error(msg, extra={"extra": extra} if extra else None)
