#!/usr/bin/env bash
# Run the full harness for a list of stacks SEQUENTIALLY (one engine at a time keeps the numbers honest:
# two benchmarks on one host steal cores from each other). Logs -> results/logs/<stack>.log.
# Usage: scripts/run-all.sh [--failover] stack1 stack2 ...
#   with no stacks: every configured stack.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FO=""
if [ "${1:-}" = "--failover" ]; then FO="--failover"; shift; fi
mkdir -p "$ROOT/results/logs"
cd "$ROOT/harness"
STACKS=("$@")
if [ ${#STACKS[@]} -eq 0 ]; then mapfile -t STACKS < <(uv run nslab list | awk '{print $1}'); fi
for s in "${STACKS[@]}"; do
  echo "=== $(date +%H:%M:%S) $s ==="
  timeout 7200 uv run nslab run "$s" $FO > "$ROOT/results/logs/$s.log" 2>&1
  echo "exit=$? $(grep -cE 'FAILED|ERR ' "$ROOT/results/logs/$s.log") failures/errors"
  docker compose -f "$ROOT/stacks/$s/compose.yaml" -p "nslab-$s" down -v -t 10 >/dev/null 2>&1 || true
done
uv run nslab report >/dev/null 2>&1 && echo "report rebuilt"
