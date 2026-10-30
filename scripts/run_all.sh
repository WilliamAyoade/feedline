#!/usr/bin/env bash
# Replay every strategy at the same speed. API and workers must already be running.
set -euo pipefail
cd "$(dirname "$0")/.."
PY="${PY:-$HOME/venvs/feedline/bin/python}"
SPEED="${SPEED:-60}"
mkdir -p results/logs

run() {
  local strategy="$1"
  local threshold="$2"
  local out="$3"
  echo "=== ${out} speed ${SPEED} ==="
  "$PY" backend/flush.py --strategy "$strategy" --threshold "$threshold"
  "$PY" backend/loadtest.py --out "$out" > "results/logs/$(basename "$out" .json).loadtest.log" 2>&1 &
  local lt=$!
  # Failed posts are recorded in the results JSON (`errors`); keep going.
  "$PY" backend/replay.py --speed "$SPEED" --wait-loadtest > "results/logs/$(basename "$out" .json).replay.log" 2>&1 \
    || echo "replay for ${out} reported failed posts"
  wait "$lt"
  "$PY" backend/verify.py --results "$out" > "results/logs/$(basename "$out" .json).verify.log" 2>&1
  echo "done ${out}"
}

# Optional arguments pick a subset, e.g. `run_all.sh hybrid:10000 push:1000`.
if [ "$#" -eq 0 ]; then
  set -- pull:1000 hybrid:1000 hybrid:10000 hybrid:100000 push:1000
fi
for spec in "$@"; do
  strategy="${spec%%:*}"
  threshold="${spec##*:}"
  if [ "$strategy" = hybrid ]; then
    out="results/hybrid-${threshold}.json"
  else
    out="results/${strategy}.json"
  fi
  run "$strategy" "$threshold" "$out"
done
echo "all strategies finished"
