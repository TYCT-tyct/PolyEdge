#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${REPO_DIR:-/home/ubuntu/PolyEdge}"
OUT_ROOT="${OUT_ROOT:-/tmp/polyedge_bg}"
ARCHIVE_ROOT="${ARCHIVE_ROOT:-$OUT_ROOT/archive}"

MODE="${MODE:-overall}" # overall|highwin
SYMBOL="${SYMBOL:-BTCUSDT}" # BTCUSDT|ETHUSDT|SOLUSDT|XRPUSDT
MARKET_TYPE="${MARKET_TYPE:-5m}"
SEED_CONFIG="${SEED_CONFIG:-configs/strategy-profiles/manual_balanced_2026_02_28.full.json}"
BASE_URL="${BASE_URL:-http://127.0.0.1:9810}"

LOOKBACKS="${LOOKBACKS:-720,1440,2880,4320}"
MAX_TRADES="${MAX_TRADES:-1100}"
MAX_SAMPLES="${MAX_SAMPLES:-260000}"
GENERATIONS="${GENERATIONS:-24}"
POPULATION="${POPULATION:-14}"
ELITISM="${ELITISM:-5}"
PHASE2_ITERS="${PHASE2_ITERS:-120}"
WORKERS="${WORKERS:-2}"
MIN_TRADES="${MIN_TRADES:-180}"
TRADE_TARGET="${TRADE_TARGET:-360}"
WIN_TARGET="${WIN_TARGET:-76}"
DD_TARGET="${DD_TARGET:-120}"
SEED="${SEED:-20260307}"
TIMEOUT="${TIMEOUT:-220}"
RETRIES="${RETRIES:-2}"
NICE_LEVEL="${NICE_LEVEL:-10}"
JOB_TAG="${JOB_TAG:-${MODE}_${MARKET_TYPE}}"

usage() {
  cat <<'USAGE'
Usage: run_lowprio_optimize.sh [--mode overall|highwin] [--symbol SYMBOL] [--market-type 5m]
                               [--seed-config PATH] [--job-tag TAG] [--max-samples N] [--workers N]

Environment overrides:
  REPO_DIR OUT_ROOT ARCHIVE_ROOT BASE_URL
  MODE SYMBOL MARKET_TYPE SEED_CONFIG LOOKBACKS MAX_TRADES MAX_SAMPLES
  GENERATIONS POPULATION ELITISM PHASE2_ITERS WORKERS
  MIN_TRADES TRADE_TARGET WIN_TARGET DD_TARGET SEED TIMEOUT RETRIES NICE_LEVEL JOB_TAG
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="${2:-}"; shift 2 ;;
    --symbol) SYMBOL="${2:-}"; shift 2 ;;
    --market-type) MARKET_TYPE="${2:-}"; shift 2 ;;
    --seed-config) SEED_CONFIG="${2:-}"; shift 2 ;;
    --job-tag) JOB_TAG="${2:-}"; shift 2 ;;
    --max-samples) MAX_SAMPLES="${2:-}"; shift 2 ;;
    --workers) WORKERS="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ "$MARKET_TYPE" != "5m" && "$MARKET_TYPE" != "15m" ]]; then
  echo "[lowprio-opt] unsupported MARKET_TYPE=$MARKET_TYPE" >&2
  exit 2
fi
if [[ "$MODE" != "overall" && "$MODE" != "highwin" ]]; then
  echo "[lowprio-opt] invalid MODE=$MODE" >&2
  exit 2
fi
if [[ "$MODE" == "highwin" ]]; then
  WIN_TARGET="${WIN_TARGET:-84}"
  DD_TARGET="${DD_TARGET:-55}"
  TRADE_TARGET="${TRADE_TARGET:-220}"
fi

mkdir -p "$OUT_ROOT" "$ARCHIVE_ROOT"
ts="$(date -u +%Y%m%dT%H%M%SZ)"
job_id="${JOB_TAG}_${SYMBOL}_${ts}"
log_file="$OUT_ROOT/${job_id}.log"
checkpoint_file="$OUT_ROOT/${job_id}.checkpoint.json"
result_file="$OUT_ROOT/${job_id}.result.json"
profile_file="$OUT_ROOT/${job_id}.profile.json"
meta_file="$OUT_ROOT/${job_id}.meta.json"
pid_file="$OUT_ROOT/${job_id}.pid"

cd "$REPO_DIR"
if [[ ! -f "$SEED_CONFIG" ]]; then
  echo "[lowprio-opt] seed config not found: $SEED_CONFIG" >&2
  exit 2
fi

ionice_prefix=()
if command -v ionice >/dev/null 2>&1; then
  ionice_prefix=(ionice -c3)
fi

cmd=(
  python3 -u scripts/forge/strategy_search_paper_full.py
  --base-url "$BASE_URL"
  --symbol "$SYMBOL"
  --market-type "$MARKET_TYPE"
  --seed-glob "configs/strategy-profiles/*.full.json"
  --extra-seed "$SEED_CONFIG"
  --lookbacks "$LOOKBACKS"
  --max-trades "$MAX_TRADES"
  --max-samples "$MAX_SAMPLES"
  --generations "$GENERATIONS"
  --population "$POPULATION"
  --elitism "$ELITISM"
  --phase2-iters "$PHASE2_ITERS"
  --workers "$WORKERS"
  --min-trades "$MIN_TRADES"
  --trade-target "$TRADE_TARGET"
  --win-target "$WIN_TARGET"
  --dd-target "$DD_TARGET"
  --timeout "$TIMEOUT"
  --retries "$RETRIES"
  --seed "$SEED"
  --checkpoint "$checkpoint_file"
  --out "$result_file"
  --save-profile "$profile_file"
)

{
  printf '{\n'
  printf '  "job_id": "%s",\n' "$job_id"
  printf '  "mode": "%s",\n' "$MODE"
  printf '  "symbol": "%s",\n' "$SYMBOL"
  printf '  "market_type": "%s",\n' "$MARKET_TYPE"
  printf '  "seed_config": "%s",\n' "$SEED_CONFIG"
  printf '  "base_url": "%s",\n' "$BASE_URL"
  printf '  "max_samples": %s,\n' "$MAX_SAMPLES"
  printf '  "workers": %s,\n' "$WORKERS"
  printf '  "started_at_utc": "%s"\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '}\n'
} > "$meta_file"

nohup "${ionice_prefix[@]}" nice -n "$NICE_LEVEL" "${cmd[@]}" >"$log_file" 2>&1 &
pid=$!
echo "$pid" > "$pid_file"

(
  while kill -0 "$pid" >/dev/null 2>&1; do
    sleep 5
  done

  end_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  archive_prefix="$ARCHIVE_ROOT/$job_id"
  cp -f "$meta_file" "${archive_prefix}.meta.json" 2>/dev/null || true
  cp -f "$log_file" "${archive_prefix}.log" 2>/dev/null || true
  cp -f "$checkpoint_file" "${archive_prefix}.checkpoint.json" 2>/dev/null || true
  cp -f "$result_file" "${archive_prefix}.result.json" 2>/dev/null || true
  cp -f "$profile_file" "${archive_prefix}.profile.json" 2>/dev/null || true

  python3 - <<PY >/dev/null 2>&1
import json
from pathlib import Path
meta = Path(r"$meta_file")
data = json.loads(meta.read_text(encoding="utf-8")) if meta.exists() else {}
data["ended_at_utc"] = r"$end_utc"
data["result_exists"] = Path(r"$result_file").exists()
data["checkpoint_exists"] = Path(r"$checkpoint_file").exists()
data["profile_exists"] = Path(r"$profile_file").exists()
meta.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
Path(r"${archive_prefix}.meta.json").write_text(
    json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
)
PY
) >/dev/null 2>&1 &

cat <<EOF
[lowprio-opt] started
job_id: $job_id
pid: $pid
mode: $MODE
log: $log_file
checkpoint: $checkpoint_file
result: $result_file
profile: $profile_file
archive_dir: $ARCHIVE_ROOT
EOF
