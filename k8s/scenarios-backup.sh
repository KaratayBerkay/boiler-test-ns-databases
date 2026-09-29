#!/usr/bin/env bash
# Backup, recovery and logging stories on the k3s runtime (ns-databases). Companion to k8s/scenarios.sh.
#
#   k8s/scenarios-backup.sh list                       which stacks have a CronJob add-on
#   k8s/scenarios-backup.sh schedule   <stack>...      kubectl apply the CronJob (k8s/addons/backup/<stack>, "0 */6 * * *")
#   k8s/scenarios-backup.sh unschedule <stack>
#   k8s/scenarios-backup.sh backup-now <stack>         create a Job from the CronJob, wait, print its log
#   k8s/scenarios-backup.sh artifacts  <stack>         ls /backups/cron inside the primary pod (ES: _cat/snapshots)
#   k8s/scenarios-backup.sh restore-drill <stack>      the harness's backup phase on the cluster (backup -> mutate -> restore -> verify)
#   k8s/scenarios-backup.sh dr-drill   <stack>         backup -> export -> delete namespace -> fresh apply -> import -> restore -> verify
#   k8s/scenarios-backup.sh logging-drill <stack>      the harness's logging phase (slow-query / audit / run-time level round trips)
#   k8s/scenarios-backup.sh promote    <stack>         the engine-level failover (kill the leader pod, measure until writes resume)
set -euo pipefail
export PATH="$HOME/.local/bin:$PATH"
K8S="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; ROOT="$(dirname "$K8S")"
set -a; . "$K8S/.env"; set +a
CTX="k3d-${CLUSTER:-nslab}"
kubectl() { command kubectl --context "$CTX" "$@"; }
ns_of() { echo "nslab-$1"; }
ADDON_STACKS="redis mongodb cassandra neo4j-cluster elasticsearch"
lab() { (cd "$ROOT" && NSLAB_PLATFORM=k3s uv run --project harness nslab "$@"); }

cmd_list() {
  echo "CronJob add-ons (k8s/addons/backup/<stack>):"
  for s in $ADDON_STACKS; do printf "  %-14s %s\n" "$s" "$( [ -f "$K8S/addons/backup/$s/cronjob.yaml" ] && echo generated || echo 'run k8s/gen-backup-addons.py')"; done
  echo "offline only (harness stops the server): neo4j (Community).  embedded, no cluster: lmdb rocksdb"
}
cmd_schedule() { for s in "$@"; do kubectl apply -k "$K8S/addons/backup/$s"; kubectl -n "$(ns_of "$s")" get cronjob "backup-$s"; done; }
cmd_unschedule() { kubectl delete -k "$K8S/addons/backup/$1" --ignore-not-found; }
wait_job() {
  local ns=$1 job=$2 t=${3:-1800}
  if ! kubectl -n "$ns" wait --for=condition=complete "job/$job" --timeout="${t}s" 2>/dev/null; then
    echo "!! job $job did not complete"; kubectl -n "$ns" logs "job/$job" --tail=40 || true; return 1
  fi
  kubectl -n "$ns" logs "job/$job" --tail=30
}
cmd_backup_now() {
  local s=$1 ns; ns=$(ns_of "$s")
  kubectl -n "$ns" get cronjob "backup-$s" >/dev/null 2>&1 || cmd_schedule "$s"
  local job="backup-$s-manual-$(date +%H%M%S)"
  kubectl -n "$ns" create job "$job" --from="cronjob/backup-$s"
  wait_job "$ns" "$job"
}
cmd_artifacts() {
  local s=$1 ns; ns=$(ns_of "$s")
  case "$s" in
    elasticsearch) kubectl -n "$ns" exec deploy/es1 -- curl -s 'localhost:9200/_cat/snapshots/nslab?v' ;;
    *) local d=""; for cand in primary mongo1 cassandra1 neo4j1 neo4j; do kubectl -n "$ns" get deploy "$cand" >/dev/null 2>&1 && { d="$cand"; break; }; done
       [ -n "$d" ] || d=$(kubectl -n "$ns" get deploy -o jsonpath='{.items[0].metadata.name}')
       kubectl -n "$ns" exec "deploy/$d" -- sh -c 'ls -la /backups/cron 2>/dev/null; du -sh /backups/cron 2>/dev/null || ls -la /backups' ;;
  esac
}
cmd="${1:-list}"; shift || true
case "$cmd" in
  list) cmd_list ;;
  schedule) cmd_schedule "$@" ;;
  unschedule) cmd_unschedule "$@" ;;
  backup-now) cmd_backup_now "$@" ;;
  artifacts) cmd_artifacts "$@" ;;
  restore-drill) lab phase "$1" backup ;;
  dr-drill) lab dr "$1" ;;
  logging-drill) lab phase "$1" logging ;;
  promote) lab run "$1" --no-up --keep --failover --phases replication ;;
  *) echo "unknown: $cmd"; sed -n '2,13p' "$0"; exit 2 ;;
esac
