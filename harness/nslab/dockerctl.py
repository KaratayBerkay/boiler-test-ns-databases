"""Thin wrappers around `docker` / `docker compose` for stack lifecycle and chaos.

With NSLAB_PLATFORM=k3s every container-level primitive delegates to `platform` (kubectl against the k3d cluster);
the phases and engine adapters do not know which runtime they are on.
"""
from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any

from . import platform


class DockerError(RuntimeError):
    pass


def _run(cmd: list[str], *, check: bool = True, timeout: int = 600, cwd: Path | None = None) -> subprocess.CompletedProcess:
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=str(cwd) if cwd else None)
    if check and p.returncode != 0:
        raise DockerError(f"{' '.join(cmd)}\nrc={p.returncode}\nstdout={p.stdout[-2000:]}\nstderr={p.stderr[-4000:]}")
    return p


def compose(stack_dir: Path, project: str, *args: str, check: bool = True, timeout: int = 900) -> subprocess.CompletedProcess:
    files = [f for f in ("compose.yaml", "compose.yml", "docker-compose.yaml", "docker-compose.yml") if (stack_dir / f).exists()]
    if not files:
        raise DockerError(f"no compose file in {stack_dir}")
    cmd = ["docker", "compose", "-f", str(stack_dir / files[0]), "-p", project, *args]
    return _run(cmd, check=check, timeout=timeout, cwd=stack_dir)


def ps(stack_dir: Path, project: str) -> list[dict[str, Any]]:
    p = compose(stack_dir, project, "ps", "-a", "--format", "json", check=False)
    out = p.stdout.strip()
    if not out:
        return []
    # compose v2 prints one JSON object per line (or a JSON array in older versions)
    if out.startswith("["):
        return json.loads(out)
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def up(stack_dir: Path, project: str, *, timeout: int = 900, oneshot: set[str] | None = None,
       pull: bool = False) -> dict[str, Any]:
    """`docker compose up -d` and wait until every service is healthy (or running when it
    has no healthcheck; or exited 0 when listed in `oneshot`)."""
    if platform.k8s():
        return platform.up(stack_dir, project, timeout=timeout, oneshot=oneshot, pull=pull)
    oneshot = oneshot or set()
    t0 = time.time()
    if pull:
        compose(stack_dir, project, "pull", "--ignore-buildable", check=False, timeout=3600)
    compose(stack_dir, project, "up", "-d", "--remove-orphans", timeout=timeout)
    last: list[dict[str, Any]] = []
    while time.time() - t0 < timeout:
        last = ps(stack_dir, project)
        pending = []
        failed = []
        for c in last:
            svc = c.get("Service") or c.get("Name")
            state = (c.get("State") or "").lower()
            health = (c.get("Health") or "").lower()
            exit_code = c.get("ExitCode", 0)
            if state == "exited":
                if svc in oneshot and exit_code == 0:
                    continue
                failed.append(f"{svc} exited({exit_code})")
            elif state == "running":
                if svc in oneshot:                      # a one-shot init job still running is not "ready"
                    pending.append(f"{svc}:running")
                    continue
                if health in ("", "healthy"):
                    continue
                pending.append(f"{svc}:{health}")
            else:
                pending.append(f"{svc}:{state}")
        if failed:
            raise DockerError(f"services failed: {failed}\n" + logs_all(stack_dir, project, tail=60))
        if not pending:
            return {"seconds": round(time.time() - t0, 1), "services": [c.get("Service") for c in last]}
        time.sleep(2)
    raise DockerError(f"timeout waiting for stack {project}: {[ (c.get('Service'), c.get('State'), c.get('Health')) for c in last]}\n" + logs_all(stack_dir, project, tail=60))


def down(stack_dir: Path, project: str, *, volumes: bool = True) -> None:
    if platform.k8s():
        return platform.down(stack_dir, project, volumes=volumes)
    args = ["down", "--remove-orphans", "-t", "20"]
    if volumes:
        args.append("-v")
    compose(stack_dir, project, *args, check=False, timeout=600)


def logs_all(stack_dir: Path, project: str, tail: int = 100) -> str:
    if platform.k8s():
        p = platform.kubectl("logs", "-n", platform._namespace(stack_dir / "k8s"), "--all-containers", "-l", "app.kubernetes.io/part-of=nslab", f"--tail={tail}", check=False)
        return (p.stdout + p.stderr)[-12000:]
    p = compose(stack_dir, project, "logs", "--no-color", "--tail", str(tail), check=False)
    return (p.stdout + p.stderr)[-12000:]


def exec_in(container: str, cmd: list[str], *, timeout: int = 300, user: str | None = None,
            check: bool = False) -> tuple[int, str, str]:
    if platform.k8s():
        return platform.exec_in(container, cmd, timeout=timeout, user=user, check=check)
    full = ["docker", "exec"]
    if user:
        full += ["-u", user]
    full += [container, *cmd]
    p = _run(full, check=check, timeout=timeout)
    return p.returncode, p.stdout, p.stderr


def container_action(container: str, action: str, *, timeout: int = 120) -> None:
    assert action in {"stop", "start", "kill", "restart", "pause", "unpause"}
    if platform.k8s():
        return platform.container_action(container, action, timeout=timeout)
    args = ["docker", action]
    if action in {"stop", "restart"}:
        args += ["-t", "15"]
    _run([*args, container], timeout=timeout)


def inspect(container: str) -> dict[str, Any]:
    if platform.k8s():
        return platform.inspect(container)
    p = _run(["docker", "inspect", container])
    return json.loads(p.stdout)[0]


def run_oneshot(image: str, cmd: list[str], *, volumes: dict[str, str], user: str | None = None,
                entrypoint: str | None = None, timeout: int = 900, network: str | None = None,
                env: dict[str, str] | None = None) -> tuple[int, str, str]:
    """`docker run --rm` a helper container over the stack's named volumes (offline file surgery on a stopped
    service: replace an AOF directory, `neo4j-admin database load`, ...). `volumes` maps volume name -> mount point.
    `network` attaches it to the stack's network (online backups that talk to a member)."""
    if platform.k8s():
        return platform.run_oneshot(image, cmd, volumes=volumes, user=user, entrypoint=entrypoint, timeout=timeout, env=env)
    full = ["docker", "run", "--rm"]
    if user:
        full += ["-u", user]
    if network:
        full += ["--network", network]
    for k, v in (env or {}).items():
        full += ["-e", f"{k}={v}"]
    if entrypoint is not None:
        full += ["--entrypoint", entrypoint]
    for vol, mnt in volumes.items():
        full += ["-v", f"{vol}:{mnt}"]
    full += [image, *cmd]
    p = _run(full, check=False, timeout=timeout)
    return p.returncode, p.stdout, p.stderr


def copy_out(container: str, src: str, dest: Path, *, timeout: int = 3600) -> None:
    """`docker cp container:src dest` (works for files and directories; dest is the target path)."""
    if platform.k8s():
        return platform.copy_out(container, src, dest, timeout=timeout)
    dest.parent.mkdir(parents=True, exist_ok=True)
    _run(["docker", "cp", f"{container}:{src}", str(dest)], timeout=timeout)


def copy_in(container: str, src: Path, dest_dir: str, *, timeout: int = 3600) -> None:
    """`docker cp src container:dest_dir/` — the copied tree lands as dest_dir/<basename> and is chmod a+rwX
    afterwards so the engine's own user (redis/mongodb/cassandra/neo4j/uid 1000) can read and move it."""
    if platform.k8s():
        return platform.copy_in(container, src, dest_dir, timeout=timeout)
    _run(["docker", "cp", str(src), f"{container}:{dest_dir.rstrip('/')}/"], timeout=timeout)
    exec_in(container, ["chmod", "-R", "a+rwX", f"{dest_dir.rstrip('/')}/{src.name}"], user="0", timeout=600)


def volume_name(project: str, volume: str) -> str:
    """Compose names a stack volume `<project>_<volume>` (unless the compose file sets `name:`); on k8s the PVC keeps the compose name."""
    return f"{project}/{volume}" if platform.k8s() else f"{project}_{volume}"


def du_bytes(container: str, path: str, *, user: str | None = "0") -> int | None:
    """Size in bytes of a path inside a container (`du -sb`), None when it does not exist."""
    rc, out, _ = exec_in(container, ["du", "-sb", path], user=user, timeout=600)
    if rc != 0 or not out.strip():
        return None
    try:
        return int(out.split()[0])
    except ValueError:
        return None


def logs(container: str, *, since: str | None = None, tail: int | None = None, timeout: int = 120) -> str:
    """stdout+stderr of a container as one string (`docker logs`)."""
    if platform.k8s():
        return platform.logs(container, since=since, tail=tail, timeout=timeout)
    cmd = ["docker", "logs"]
    if since:
        cmd += ["--since", since]
    if tail is not None:
        cmd += ["--tail", str(tail)]
    cmd.append(container)
    p = _run(cmd, check=False, timeout=timeout)
    return p.stdout + p.stderr


def log_facts(container: str, *, sample_lines: int = 200) -> dict[str, Any]:
    """Logging-driver configuration and the volume/shape of what the container has logged so far."""
    try:
        d = inspect(container)
    except DockerError as e:
        return {"error": str(e)[:200]}
    lc = d.get("HostConfig", {}).get("LogConfig", {}) or {}
    out: dict[str, Any] = {"driver": lc.get("Type"), "options": lc.get("Config") or {}, "log_path": d.get("LogPath"),
                           "user": d.get("Config", {}).get("User") or "root", "started": d.get("State", {}).get("StartedAt")}
    txt = logs(container)
    lines = [ln for ln in txt.splitlines() if ln.strip()]
    out["bytes"] = len(txt.encode("utf-8", "replace"))
    out["lines"] = len(lines)
    tail = lines[-sample_lines:]
    js = 0
    for ln in tail:
        st = ln.lstrip()
        if st.startswith("{") and st.endswith("}"):
            try:
                json.loads(st)
                js += 1
            except json.JSONDecodeError:
                pass
    out["json_lines_pct"] = round(100 * js / len(tail), 1) if tail else None
    out["format"] = "json" if tail and js / len(tail) > 0.8 else ("mixed" if js else "text")
    out["sample"] = [ln[:240] for ln in tail[-3:]]
    return out


def stats(containers: list[str]) -> list[dict[str, Any]]:
    if not containers:
        return []
    if platform.k8s():
        return []
    p = _run(["docker", "stats", "--no-stream", "--format", "json", *containers], check=False, timeout=60)
    out = []
    for line in p.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            continue
        out.append({"name": d.get("Name"), "cpu": d.get("CPUPerc"), "mem": d.get("MemUsage"),
                    "mem_pct": d.get("MemPerc"), "net": d.get("NetIO"), "block": d.get("BlockIO"), "pids": d.get("PIDs")})
    return out


def image_size(image: str) -> str | None:
    p = _run(["docker", "image", "inspect", image, "--format", "{{.Size}}"], check=False)
    if p.returncode != 0:
        return None
    try:
        b = int(p.stdout.strip())
        return f"{b/1e9:.2f} GB" if b > 1e9 else f"{b/1e6:.0f} MB"
    except ValueError:
        return None


def wait_for(predicate, *, timeout: float, interval: float = 0.5, desc: str = "condition") -> float:
    t0 = time.time()
    while True:
        try:
            if predicate():
                return time.time() - t0
        except Exception:  # noqa: BLE001
            pass
        if time.time() - t0 > timeout:
            raise TimeoutError(f"timed out after {timeout}s waiting for {desc}")
        time.sleep(interval)


# ---------------------------------------------------------------------------------------
# cgroup v2 resource accounting (exact CPU-seconds and memory per container, no sampling noise)
_CG_ROOT = Path("/sys/fs/cgroup/system.slice")


def cgroup_paths(containers: list[str]) -> dict[str, Path]:
    out = {}
    if platform.k8s():
        return out                  # the containers live inside the k3d node: no host cgroup accounting per container
    for name in containers:
        p = _run(["docker", "inspect", name, "--format", "{{.Id}}"], check=False)
        cid = p.stdout.strip()
        if not cid:
            continue
        d = _CG_ROOT / f"docker-{cid}.scope"
        if d.exists():
            out[name] = d
    return out


def cgroup_snapshot(paths: dict[str, Path]) -> dict[str, dict[str, float]]:
    snap = {}
    for name, d in paths.items():
        try:
            usage = 0.0
            for line in (d / "cpu.stat").read_text().splitlines():
                if line.startswith("usage_usec"):
                    usage = float(line.split()[1])
            quota = (d / "cpu.max").read_text().split()
            cpu_limit = float(quota[0]) / float(quota[1]) if quota[0] != "max" else None
            mem_max = (d / "memory.max").read_text().strip()
            snap[name] = {"usage_usec": usage, "cpu_limit_cores": cpu_limit, "mem_current": float((d / "memory.current").read_text()),
                          "mem_peak": float((d / "memory.peak").read_text()) if (d / "memory.peak").exists() else None,
                          "mem_limit": float(mem_max) if mem_max != "max" else None, "t": time.time()}
        except Exception:  # noqa: BLE001
            pass
    return snap


def cgroup_delta(before: dict[str, dict[str, float]], after: dict[str, dict[str, float]]) -> dict[str, dict[str, float | None]]:
    out = {}
    for name, a in after.items():
        b = before.get(name)
        if not b:
            continue
        el = a["t"] - b["t"]
        cores = (a["usage_usec"] - b["usage_usec"]) / 1e6 / el if el > 0 else 0.0
        lim = a.get("cpu_limit_cores")
        out[name] = {"cpu_cores_avg": round(cores, 3), "cpu_limit_cores": lim, "cpu_pct_of_limit": round(100 * cores / lim, 1) if lim else None,
                     "mem_peak_mb": round(a["mem_peak"] / 1048576) if a.get("mem_peak") else None, "mem_end_mb": round(a["mem_current"] / 1048576),
                     "mem_limit_mb": round(a["mem_limit"] / 1048576) if a.get("mem_limit") else None}
    return out
