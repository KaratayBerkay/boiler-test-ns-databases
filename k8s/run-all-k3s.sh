#!/usr/bin/env bash
# Run the whole lab on the k3d cluster, one stack at a time (the same rule as scripts/run-all.sh: two benchmarks on one
# host distort each other): every phase + failover, then the disaster-recovery drill, then the namespace is deleted.
# Logs -> results/k3s/logs/<stack>.log. Usage: k8s/run-all-k3s.sh [--no-dr|--dr-only] [stack ...]   (default: every compose stack)
# The DR drill runs on a FRESH deployment (namespace deleted, apply, load): after `--failover` the old primary may have
# been demoted for good (Redis Sentinel) or not yet re-elected (MongoDB priorities), so a drill on that stack would lie.
set -uo pipefail
export PATH="$HOME/.local/bin:$PATH"
K8S="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; ROOT="$(dirname "$K8S")"
export NSLAB_PLATFORM=k3s
DR=true; RUN=true
case "${1:-}" in --no-dr) DR=false; shift ;; --dr-only) RUN=false; shift ;; esac
STACKS=("$@")
if [ ${#STACKS[@]} -eq 0 ]; then mapfile -t STACKS < <(for d in "$ROOT"/stacks/*/k8s/kustomization.yaml; do basename "$(dirname "$(dirname "$d")")"; done); fi
mkdir -p "$ROOT/results/k3s/logs"
cd "$ROOT/harness"
for s in "${STACKS[@]}"; do
  echo "=== $(date +%H:%M:%S) $s (k3s) ==="
  if $RUN; then
    timeout 7200 uv run nslab run "$s" --failover --keep > "$ROOT/results/k3s/logs/$s.log" 2>&1
    echo "run exit=$? $(grep -cE 'FAILED|ERR ' "$ROOT/results/k3s/logs/$s.log") failures/errors"
    uv run nslab down "$s" >/dev/null 2>&1 || true
  fi
  if $DR; then
    timeout 3600 uv run nslab run "$s" --keep --phases load > "$ROOT/results/k3s/logs/$s-dr.log" 2>&1     # fresh deployment + dataset
    timeout 3600 uv run nslab dr "$s" >> "$ROOT/results/k3s/logs/$s-dr.log" 2>&1
    echo "dr exit=$? $(grep -oE 'verified=(True|False)' "$ROOT/results/k3s/logs/$s-dr.log" | tail -1)"
    uv run nslab down "$s" >/dev/null 2>&1 || true
  fi
done
uv run nslab report >/dev/null 2>&1 && echo "report rebuilt"
