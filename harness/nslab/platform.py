"""Run the harness against Docker Compose (default) or the k3s/k3d cluster (NSLAB_PLATFORM=k3s).

The container-level primitives in dockerctl delegate here when NSLAB_PLATFORM=k3s. Everything is keyed on the compose
container name, which k8s/gen.py stamps on every pod as the label `nslab.io/container`; `dockerctl.exec_in(
"nslab-redis-primary", ...)` finds that pod in whatever namespace it lives and `kubectl exec`s into it. What the
phases do through dockerctl then works unchanged: exec, logs, du, stop/start/kill (failover, offline backups),
up/down (kubectl apply/delete of stacks/<key>/k8s), docker cp (kubectl cp), one-shot helper containers over the
data volumes (a throw-away Pod mounting the same PersistentVolumeClaims), and `inspect` (a docker-shaped view of
the pod: mounts -> PVC names, image, compose labels).

Not emulated, reported honestly: cgroup CPU/memory accounting (the containers live inside the k3d node; the load
test reports docs/s without per-container cores) and docker `pause`.
"""
from __future__ import annotations

import json
import os
import subprocess
import time
from functools import lru_cache
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
PLATFORM = os.environ.get("NSLAB_PLATFORM", "compose").lower()
CONTEXT = os.environ.get("NSLAB_K8S_CONTEXT", f"k3d-{os.environ.get('CLUSTER', 'nslab')}")
LABEL = "nslab.io/container"


class PlatformUnsupported(RuntimeError):
    """A primitive this platform does not provide."""


def k8s() -> bool:
    return PLATFORM == "k3s"


def kubectl(*args: str, check: bool = True, timeout: int = 120, input: str | None = None) -> subprocess.CompletedProcess:
    p = subprocess.run(["kubectl", "--context", CONTEXT, *args], capture_output=True, text=True, timeout=timeout, input=input)
    if check and p.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(args)}\nrc={p.returncode}\n{p.stdout[-1500:]}\n{p.stderr[-2500:]}")
    return p


@lru_cache(maxsize=256)
def _resolve(container: str, _generation: int = 0) -> tuple[str, str]:
    """(namespace, pod) for a compose container name via the nslab.io/container label; a Running pod wins."""
    p = kubectl("get", "pod", "-A", "-l", f"{LABEL}={container}",
                "-o", "jsonpath={range .items[*]}{.metadata.namespace} {.metadata.name} {.status.phase}\n{end}", check=False)
    rows = [ln.split() for ln in p.stdout.splitlines() if len(ln.split()) == 3]
    if not rows:
        raise RuntimeError(f"no pod with {LABEL}={container} (is the stack deployed? kubectl get pod -A -l {LABEL}={container})")
    for ns, pod, phase in rows:
        if phase == "Running":
            return ns, pod
    return rows[0][0], rows[0][1]


_GEN = 0


def _pod(container: str) -> tuple[str, str]:
    return _resolve(container, _GEN)


def bust_pod_cache() -> None:
    global _GEN
    _GEN += 1


# --- primitives dockerctl delegates to -------------------------------------------------------------------------
def exec_in(container: str, cmd: list[str], *, timeout: int = 300, user: str | None = None, check: bool = False):
    ns, pod = _pod(container)
    # kubectl exec has no -u: the pod's securityContext decides. The lab's images run their servers as an
    # unprivileged user but the pod's default exec user is root for the official images, which is what `user="0"` wants.
    p = kubectl("exec", "-n", ns, pod, "--", *cmd, check=False, timeout=timeout)
    if check and p.returncode != 0:
        raise RuntimeError(f"kubectl exec {pod} {' '.join(cmd)} rc={p.returncode}: {p.stderr[-800:]}")
    return p.returncode, p.stdout, p.stderr


def logs(container: str, *, since: str | None = None, tail: int | None = None, timeout: int = 120) -> str:
    try:
        ns, pod = _pod(container)
    except RuntimeError as e:
        return str(e)
    args = ["logs", "-n", ns, pod]
    if since:
        args.append(f"--since-time={since}")
    if tail is not None:
        args.append(f"--tail={tail}")
    p = kubectl(*args, check=False, timeout=timeout)
    return p.stdout + p.stderr


def _deployment(container: str) -> tuple[str, str]:
    """(namespace, deployment) for a compose container name — the Deployment carries the same label as its pods, so
    this resolves even while the Deployment is scaled to 0 (a stopped service has no pod)."""
    p = kubectl("get", "deploy", "-A", "-l", f"{LABEL}={container}", "-o", "jsonpath={range .items[*]}{.metadata.namespace} {.metadata.name}\n{end}", check=False)
    rows = [ln.split() for ln in p.stdout.splitlines() if len(ln.split()) == 2]
    if not rows:
        raise RuntimeError(f"no Deployment with {LABEL}={container}")
    return rows[0][0], rows[0][1]


def container_action(container: str, action: str, *, timeout: int = 120) -> None:
    """stop: scale the owning Deployment to 0 (SIGTERM, graceful — the member steps down cleanly);
    kill: SIGKILL the database process first (`docker kill` semantics: the other members must *detect* the loss),
    then scale to 0 so the kubelet does not restart it; start: back to 1; restart: rollout."""
    ns, owner = _deployment(container)
    if action in ("stop", "kill"):
        try:
            _, pod = _pod(container)
        except RuntimeError:
            pod = None
        if action == "kill" and pod:
            kubectl("exec", "-n", ns, pod, "--", "kill", "-9", "1", check=False, timeout=30)
        kubectl("scale", "-n", ns, f"deployment/{owner}", "--replicas=0", timeout=timeout)
        if pod:
            if action == "kill":
                kubectl("delete", "pod", "-n", ns, pod, "--grace-period=0", "--force", check=False, timeout=timeout)
            kubectl("wait", "-n", ns, "--for=delete", f"pod/{pod}", f"--timeout={timeout}s", check=False)
    elif action == "start":
        kubectl("scale", "-n", ns, f"deployment/{owner}", "--replicas=1", timeout=timeout)
        kubectl("rollout", "status", "-n", ns, f"deployment/{owner}", f"--timeout={timeout}s", check=False)
    elif action == "restart":
        kubectl("rollout", "restart", "-n", ns, f"deployment/{owner}", timeout=timeout)
        kubectl("rollout", "status", "-n", ns, f"deployment/{owner}", f"--timeout={timeout}s", check=False)
    else:
        raise PlatformUnsupported(f"{action} not supported on k8s")
    bust_pod_cache()


def _deploy_of(ns: str, pod: str) -> str:
    p = kubectl("get", "pod", "-n", ns, pod, "-o", "jsonpath={.metadata.labels.app\\.kubernetes\\.io/name}", check=False)
    return p.stdout.strip() or pod.rsplit("-", 2)[0]


def inspect(container: str) -> dict[str, Any]:
    """A docker-inspect-shaped view of the pod: Mounts (PVC claim name as Name), Config.Image/Labels, State, NetworkSettings."""
    ns, pod = _pod(container)
    p = kubectl("get", "pod", "-n", ns, pod, "-o", "json")
    d = json.loads(p.stdout)
    spec, status = d.get("spec", {}), d.get("status", {})
    claims = {v["name"]: v["persistentVolumeClaim"]["claimName"] for v in spec.get("volumes", []) if v.get("persistentVolumeClaim")}
    c = spec["containers"][0]
    mounts = [{"Destination": m["mountPath"], "Name": f"{ns}/{claims[m['name']]}", "Type": "volume"} for m in c.get("volumeMounts", []) if m["name"] in claims]
    labels = dict(d["metadata"].get("labels", {}))
    labels["com.docker.compose.service"] = labels.get("app.kubernetes.io/name", "")
    return {"Id": d["metadata"]["uid"], "Name": pod, "Mounts": mounts,
            "Config": {"Image": c.get("image"), "Labels": labels, "Cmd": list(c.get("args") or []), "User": str((c.get("securityContext") or {}).get("runAsUser", "")), "Hostname": spec.get("hostname", pod)},
            "State": {"StartedAt": (status.get("startTime") or ""), "Status": status.get("phase", "").lower()},
            "HostConfig": {"LogConfig": {"Type": "kubelet (container-log-max-size/-files via k3d.yaml)", "Config": {"max-size": "50Mi", "max-file": "3"}}},
            "LogPath": f"/var/log/pods/{ns}_{pod}_{d['metadata']['uid']}/",
            "NetworkSettings": {"Networks": {ns: {"IPAddress": status.get("podIP", "")}}}}


def run_oneshot(image: str, cmd: list[str], *, volumes: dict[str, str], user: str | None = None,
                entrypoint: str | None = None, timeout: int = 900, env: dict[str, str] | None = None) -> tuple[int, str, str]:
    """A throw-away Pod over the given PVCs (volume keys are `<namespace>/<claim>`, as inspect() reports them),
    run to completion; returns (rc, logs, ''). RWO claims are fine: the lab cluster is one node."""
    nss = {v.split("/", 1)[0] for v in volumes}
    if len(nss) != 1:
        raise PlatformUnsupported(f"one-shot pod needs volumes from one namespace, got {sorted(nss)}")
    ns = nss.pop()
    name = f"nslab-oneshot-{int(time.time() * 1000) % 10_000_000}"
    vols = [{"name": f"v{i}", "persistentVolumeClaim": {"claimName": v.split('/', 1)[1]}} for i, v in enumerate(volumes)]
    mounts = [{"name": f"v{i}", "mountPath": mp} for i, mp in enumerate(volumes.values())]
    c: dict[str, Any] = {"name": "job", "image": image, "imagePullPolicy": "IfNotPresent", "volumeMounts": mounts}
    if env:
        c["env"] = [{"name": k, "value": str(v)} for k, v in env.items()]
    if entrypoint is not None:
        c["command"] = [entrypoint]
        c["args"] = cmd
    else:
        c["args"] = cmd
    if user is not None and str(user).isdigit():
        c["securityContext"] = {"runAsUser": int(user)}
    elif user in ("neo4j",):
        c["securityContext"] = {"runAsUser": 7474, "runAsGroup": 7474}
    elif user == "redis":
        c["securityContext"] = {"runAsUser": 999}
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "namespace": ns, "labels": {"app.kubernetes.io/part-of": "nslab", "nslab.io/oneshot": "true"}},
           "spec": {"restartPolicy": "Never", "containers": [c], "volumes": vols}}
    kubectl("apply", "-f", "-", input=json.dumps(pod), timeout=60)
    try:
        t0 = time.time()
        phase = ""
        while time.time() - t0 < timeout:
            p = kubectl("get", "pod", "-n", ns, name, "-o", "jsonpath={.status.phase}", check=False)
            phase = p.stdout.strip()
            if phase in ("Succeeded", "Failed"):
                break
            time.sleep(1)
        out = kubectl("logs", "-n", ns, name, check=False).stdout
        rc = 0 if phase == "Succeeded" else 1
        if phase not in ("Succeeded", "Failed"):
            out += f"\n(timed out after {timeout}s in phase {phase})"
        return rc, out, ""
    finally:
        kubectl("delete", "pod", "-n", ns, name, "--ignore-not-found", "--wait=false", check=False)


def image_of(container: str) -> str:
    return inspect(container)["Config"]["Image"]


def _has_tar(container: str) -> bool:
    rc, _, _ = exec_in(container, ["tar", "--version"], timeout=30)
    return rc == 0


def _helper_image() -> str:
    """The busybox image mirrored in Harbor (k8s/images.lock.json), for helper pods that need tar."""
    f = ROOT / "k8s" / "images.lock.json"
    if f.exists():
        for src, v in json.loads(f.read_text()).get("images", {}).items():
            if src.startswith("busybox") and v.get("harbor"):
                return v["harbor"]
    return "busybox:1.37"


def _pvc_for(container: str, path: str) -> tuple[str, str, str]:
    """(namespace, claim, mountPath) of the PVC that holds `path` in the pod (longest mountPath prefix)."""
    ns, _ = _pod(container)
    mounts = sorted(inspect(container)["Mounts"], key=lambda m: -len(m["Destination"]))
    for m in mounts:
        if path == m["Destination"] or path.startswith(m["Destination"].rstrip("/") + "/"):
            return ns, m["Name"].split("/", 1)[1], m["Destination"]
    raise PlatformUnsupported(f"{path} is not on a PersistentVolumeClaim of {container}")


class _Helper:
    """A sleeping busybox pod over one PVC — `kubectl cp` needs tar in the target, which some images (Elasticsearch) lack."""

    def __init__(self, ns: str, claim: str, mount: str):
        self.ns, self.name = ns, f"nslab-cp-{int(time.time() * 1000) % 10_000_000}"
        pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": self.name, "namespace": ns, "labels": {"app.kubernetes.io/part-of": "nslab", "nslab.io/oneshot": "true"}},
               "spec": {"restartPolicy": "Never", "containers": [{"name": "cp", "image": _helper_image(), "command": ["sh", "-c", "sleep 3600"],
                                                                  "volumeMounts": [{"name": "v", "mountPath": mount}]}],
                        "volumes": [{"name": "v", "persistentVolumeClaim": {"claimName": claim}}]}}
        kubectl("apply", "-f", "-", input=json.dumps(pod), timeout=60)
        kubectl("wait", "-n", ns, "--for=condition=Ready", f"pod/{self.name}", "--timeout=120s")

    def __enter__(self):
        return self

    def __exit__(self, *a):
        kubectl("delete", "pod", "-n", self.ns, self.name, "--ignore-not-found", "--wait=false", check=False)


def copy_out(container: str, src: str, dest: Path, *, timeout: int = 3600) -> None:
    ns, pod = _pod(container)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if _has_tar(container):
        kubectl("cp", f"{ns}/{pod}:{src}", str(dest), timeout=timeout)
        return
    ns, claim, mount = _pvc_for(container, src)
    with _Helper(ns, claim, mount) as h:
        kubectl("cp", f"{ns}/{h.name}:{src}", str(dest), timeout=timeout)


def copy_in(container: str, src: Path, dest_dir: str, *, timeout: int = 3600) -> None:
    ns, pod = _pod(container)
    target = f"{dest_dir.rstrip('/')}/{src.name}"
    if _has_tar(container):
        kubectl("cp", str(src), f"{ns}/{pod}:{target}", timeout=timeout)
        exec_in(container, ["chmod", "-R", "a+rwX", target], timeout=600)
        return
    ns, claim, mount = _pvc_for(container, dest_dir)
    with _Helper(ns, claim, mount) as h:
        kubectl("cp", str(src), f"{ns}/{h.name}:{target}", timeout=timeout)
        kubectl("exec", "-n", ns, h.name, "--", "chmod", "-R", "a+rwX", target, check=False, timeout=600)


# --- stack lifecycle --------------------------------------------------------------------------------------------
def _kdir(stack_dir: Path) -> Path:
    return stack_dir / "k8s"


def _namespace(kdir: Path) -> str:
    import yaml
    k = yaml.safe_load((kdir / "kustomization.yaml").read_text())
    return k.get("namespace") or f"nslab-{kdir.parent.name}"


def up(stack_dir: Path, project: str, *, timeout: int = 900, oneshot=None, pull: bool = False) -> dict[str, Any]:
    kdir = _kdir(stack_dir)
    if not (kdir / "kustomization.yaml").exists():
        raise PlatformUnsupported(f"no k8s manifests for {stack_dir.name}: make -C k8s gen")
    t0 = time.time()
    kubectl("apply", "-k", str(kdir), timeout=300)
    ns = _namespace(kdir)
    deploys = kubectl("get", "deploy", "-n", ns, "-o", "jsonpath={.items[*].metadata.name}", check=False).stdout.split()
    for d in deploys:
        p = kubectl("rollout", "status", "-n", ns, f"deployment/{d}", f"--timeout={timeout}s", check=False, timeout=timeout + 60)
        if p.returncode != 0:
            raise RuntimeError(f"deployment {d} not available within {timeout}s:\n{p.stdout[-800:]}{p.stderr[-800:]}\n"
                               + kubectl("get", "pods", "-n", ns, check=False).stdout)
    jobs = kubectl("get", "job", "-n", ns, "-o", "jsonpath={.items[*].metadata.name}", check=False).stdout.split()
    for j in jobs:
        kubectl("wait", "-n", ns, "--for=condition=complete", f"job/{j}", f"--timeout={timeout}s", check=False, timeout=timeout + 60)
    bust_pod_cache()
    return {"seconds": round(time.time() - t0, 1), "services": deploys + jobs, "namespace": ns}


def down(stack_dir: Path, project: str, *, volumes: bool = True) -> None:
    kdir = _kdir(stack_dir)
    ns = _namespace(kdir)
    if volumes:
        kubectl("delete", "namespace", ns, "--ignore-not-found", "--wait=true", timeout=900, check=False)
    else:
        kubectl("delete", "-k", str(kdir), "--ignore-not-found", check=False, timeout=300)
    bust_pod_cache()


# --- ports: lab.yaml host port -> nodePort published on 127.0.0.1 ----------------------------------------------
@lru_cache(maxsize=1)
def _port_table() -> dict[str, int]:
    f = ROOT / "k8s" / "ports.json"
    if not f.exists():
        return {}
    return {hp: v["nodePort"] for hp, v in json.loads(f.read_text())["ports"].items()}


def map_port(host_port: int) -> int:
    return _port_table().get(str(host_port), host_port)
