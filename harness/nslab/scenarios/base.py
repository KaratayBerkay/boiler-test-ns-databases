"""Shared scenario primitives + registry, so the load driver can run any scenario, not just messaging."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class Op:
    name: str
    weight: float
    kind: str                            # read | write
    run: Callable[["Ctx"], Any]          # picks targets from the corpus and calls the backend
    tags: tuple[str, ...] = ()


@dataclass
class Ctx:
    backend: Any
    corpus: Any
    rng: Any


@dataclass
class Scenario:
    name: str
    sizes_named: Callable[[str], Any]                 # (size_name) -> Sizes
    make_corpus: Callable[[Any], Any]                 # (sizes) -> corpus
    make_backend: Callable[[str, Any, dict], Any]     # (stack_key, cfg, opts) -> backend (has .connect()/.close())
    mix: Callable[[], list[Op]]                       # () -> list[Op]
    seed: Callable[..., dict]                         # (stack_key, size, workers, log) -> dict
    curves: Callable[..., dict]                       # (stack_key, size, opts, log) -> dict
    default_opts: dict = field(default_factory=dict)
    blurb: str = ""


_REGISTRY: dict[str, Scenario] = {}


def register(scen: Scenario) -> None:
    _REGISTRY[scen.name] = scen


def get(name: str) -> Scenario:
    if name not in _REGISTRY:
        _load_all()
    if name not in _REGISTRY:
        raise KeyError(f"unknown scenario '{name}' (known: {sorted(_REGISTRY)})")
    return _REGISTRY[name]


def names() -> list[str]:
    _load_all()
    return sorted(_REGISTRY)


def _load_all() -> None:
    import importlib
    for mod in ("messaging", "recgraph"):
        try:
            importlib.import_module(f"{__package__}.{mod}")
        except Exception:  # noqa: BLE001 - a scenario with a missing driver should not break the others
            pass
