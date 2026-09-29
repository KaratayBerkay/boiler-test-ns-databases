#!/usr/bin/env python3
"""Kubernetes-native backup add-ons for the k3s runtime: one CronJob per stack that writes a native backup into the
stack's shared `backups` PersistentVolumeClaim (the same volume the harness's backup phase and DR drill use), with
retention. Same shape as rd-databases/k8s/gen-backup-addons.py.

Written as standalone kustomize directories under k8s/addons/backup/<stack>/ (namespace = the stack's namespace),
applied on top of the generated stack manifests: they reference the PVCs and Services gen.py creates but never touch
those files, so regenerating the stack manifests does not disturb them and vice versa.

  gen-backup-addons.py              write k8s/addons/backup/<stack>/ for every stack with an online backup
  gen-backup-addons.py redis ...    only these
  k8s/scenarios-backup.sh schedule|backup-now|artifacts|restore-drill|dr-drill ...   drives them

Each CronJob runs the engine's own image (already mirrored in Harbor), connects through the Service DNS name of the
primary, writes /backups/cron/<timestamp>* and keeps the newest KEEP artifacts — the "schedule" and "retention" rows of
the BACKUP_STRATEGY every adapter declares, made concrete.
"""
from __future__ import annotations

import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
K8S = ROOT / "k8s"
sys.path.insert(0, str(K8S))
import gen  # noqa: E402  (harbor_ref, Dumper)

KEEP = 5
SCHEDULE = "0 */6 * * *"
RETENTION = 'ls -1dt /backups/cron/* 2>/dev/null | tail -n +$((KEEP+1)) | xargs -r rm -rf; echo "kept:"; ls -la /backups/cron'

# stack -> image, extra PVCs (name -> mount), user, env, shell script. $TS = timestamp, /backups = the shared volume.
SPECS: dict[str, dict] = {
    "redis": {"image": "redis:8",
              # redis-cli --rdb: the client asks the primary for a SYNC stream and writes the RDB itself — no access to
              # the server's /data needed, and it works with an AOF-only primary (BGSAVE would land in the server's volume)
              "script": "redis-cli -h primary --rdb /backups/cron/$TS.rdb && ls -la /backups/cron/$TS.rdb"},
    "mongodb": {"image": "mongo:7",
                "script": "mongodump --host=mongo1:27017 --oplog --gzip --archive=/backups/cron/$TS.archive.gz && ls -la /backups/cron/$TS.archive.gz"},
    "cassandra": {"image": "cassandra:5", "pvcs": {"cass1": "/data1", "cass2": "/data2", "cass3": "/data3"},
                  # nodetool over JMX (opened by the x-k8s hint in stacks/cassandra/compose.yaml), then export every
                  # node's snapshot from its data PVC in the <keyspace>/<table>/ layout nodetool import / sstableloader expect
                  "script": ('for n in 1 2 3; do nodetool -h cassandra$n flush lab && nodetool -h cassandra$n snapshot -t cron-$TS --ttl 24h lab; done; '
                             'for n in 1 2 3; do for d in /data$n/data/lab/*/snapshots/cron-$TS; do tb=$(basename $(dirname $(dirname $d))); tb=${tb%-*}; '
                             'mkdir -p /backups/cron/$TS/node$n/lab/$tb; cp -a "$d"/. /backups/cron/$TS/node$n/lab/$tb/; done; done; du -sh /backups/cron/$TS')},
    "neo4j-cluster": {"image": "neo4j:5-enterprise", "user": 7474, "env": {"NEO4J_ACCEPT_LICENSE_AGREEMENT": "eval"},
                      "script": "mkdir -p /backups/cron/$TS && neo4j-admin database backup lab --from=neo4j1:6362 --to-path=/backups/cron/$TS --type=full && ls -la /backups/cron/$TS"},
    "elasticsearch": {"image": "elasticsearch:9.5.3", "user": 1000, "pvcs": {},       # the repository is server-side on the shared volume
                      "script": ('curl -sf -XPUT es1:9200/_snapshot/nslab -H "Content-Type: application/json" -d \'{"type":"fs","settings":{"location":"/backups/nslab","compress":true}}\' >/dev/null && '
                                 'curl -sf -XPUT "es1:9200/_snapshot/nslab/cron-$TS?wait_for_completion=true" -H "Content-Type: application/json" -d \'{"indices":"lab_*","include_global_state":false}\' && echo && '
                                 'for old in $(curl -sf "es1:9200/_cat/snapshots/nslab?h=id&s=start_epoch:desc" | grep ^cron- | tail -n +$((KEEP+1))); do curl -sf -XDELETE "es1:9200/_snapshot/nslab/$old" >/dev/null && echo "deleted $old"; done; '
                                 'curl -sf "es1:9200/_cat/snapshots/nslab?v"')},
}
# neo4j (Community): no online backup — the harness's backup phase stops the server for `neo4j-admin database dump`
#   (NSLAB_PLATFORM=k3s nslab phase neo4j backup); a CronJob cannot do that safely. lmdb/rocksdb: embedded, no cluster.


def cronjob(stack: str, spec: dict) -> dict:
    ns = f"nslab-{stack}"
    es = stack == "elasticsearch"
    script = f'set -euo pipefail; TS=$(date +%Y%m%d-%H%M%S); {"" if es else "mkdir -p /backups/cron; "}echo "== backup {stack} $TS"; {spec["script"]}; echo "== done"; '
    if not es:
        script += RETENTION
    container = {"name": "backup", "image": gen.harbor_ref(spec["image"]), "imagePullPolicy": "IfNotPresent",
                 "command": ["bash", "-c", script],
                 "env": [{"name": "KEEP", "value": str(KEEP)}] + [{"name": k, "value": v} for k, v in (spec.get("env") or {}).items()]}
    if spec.get("user"):
        container["securityContext"] = {"runAsUser": spec["user"]}
    volumes, mounts = [], []
    if not es:
        volumes.append({"name": "backups", "persistentVolumeClaim": {"claimName": "backups"}})
        mounts.append({"name": "backups", "mountPath": "/backups"})
    for i, (claim, mp) in enumerate((spec.get("pvcs") or {}).items()):
        volumes.append({"name": f"data{i}", "persistentVolumeClaim": {"claimName": claim}})
        mounts.append({"name": f"data{i}", "mountPath": mp, "readOnly": True})
    container["volumeMounts"] = mounts
    labels = {"app.kubernetes.io/name": f"backup-{stack}", "app.kubernetes.io/part-of": "nslab", "nslab.io/stack": stack, "nslab.io/addon": "backup"}
    return {"apiVersion": "batch/v1", "kind": "CronJob",
            "metadata": {"name": f"backup-{stack}", "namespace": ns, "labels": labels},
            "spec": {"schedule": SCHEDULE, "concurrencyPolicy": "Forbid", "successfulJobsHistoryLimit": 3, "failedJobsHistoryLimit": 3,
                     "jobTemplate": {"spec": {"backoffLimit": 1, "ttlSecondsAfterFinished": 86400,
                                              "template": {"metadata": {"labels": labels},
                                                           "spec": {"restartPolicy": "Never", "containers": [container], "volumes": volumes}}}}}}


def write(stack: str) -> Path:
    d = K8S / "addons" / "backup" / stack
    d.mkdir(parents=True, exist_ok=True)
    (d / "cronjob.yaml").write_text("# generated by k8s/gen-backup-addons.py -- scheduled backup into the stack's backups PVC\n" +
                                    yaml.dump(cronjob(stack, SPECS[stack]), Dumper=gen.Dumper, sort_keys=False, width=100000))
    (d / "kustomization.yaml").write_text(yaml.dump({"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization",
                                                     "namespace": f"nslab-{stack}", "resources": ["cronjob.yaml"]}, Dumper=gen.Dumper, sort_keys=False))
    return d


def main() -> None:
    keys = sys.argv[1:] or sorted(SPECS)
    for k in keys:
        if k not in SPECS:
            sys.exit(f"no backup add-on spec for {k} (neo4j Community backs up offline via the harness; lmdb/rocksdb are embedded)")
        print(f"wrote {write(k).relative_to(ROOT)}")


if __name__ == "__main__":
    main()
