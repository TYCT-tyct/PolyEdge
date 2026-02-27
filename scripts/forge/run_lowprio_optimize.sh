#!/usr/bin/env bash
set -euo pipefail

# Low-priority optimizer launcher for Ireland API.
# - runs search in background with nice/ionice
# - enforces bounded max-samples + bounded workers
# - auto archives outputs after completion

REPO_DIR="${REPO_DIR:-/home/ubuntu/PolyEdge}"
OUT_ROOT="${OUT_ROOT:-/tmp/polyedge_bg}"
ARCHIVE_ROOT="${ARCHIVE_ROOT:-$OUT_ROOT/archive}"

MODE="${MODE:-parallel}" # parallel|refine
MARKET_TYPE="${MARKET_TYPE:-5m}" # 5m only in current production phase
SEED_CONFIG="${SEED_CONFIG:-configs/strategy-profiles/manual_hi_freq_2026_02_27.full.json}"
BASE_URL="${BASE_URL:-http://127.0.0.1:9810}"

LOOKBACKS="${LOOKBACKS:-720,1440,2880}"
MAX_TRADES="${MAX_TRADES:-900}"
MAX_SAMPLES="${MAX_SAMPLES:-260000}"

WORKERS="${WORKERS:-2}"
GENERATIONS="${GENERATIONS:-14}"
POPULATION="${POPULATION:-10}"
MIN_TRADES="${MIN_TRADES:-220}"
TRADE_TARGET="${TRADE_TARGET:-450}"
WIN_FLOOR="${WIN_FLOOR:-88}"
SEED="${SEED:-20260227}"

REFINE_ITERS="${REFINE_ITERS:-120}"
REFINE_LOOKBACK_MINUTES="${REFINE_LOOKBACK_MINUTES:-1440}"
REFINE_MAX_TRADES="${REFINE_MAX_TRADES:-900}"
REFINE_MIN_TRADES="${REFINE_MIN_TRADES:-120}"
REFINE_WIN_FLOOR="${REFINE_WIN_FLOOR:-84}"
RETRIES="${RETRIES:-2}"
TIMEOUT="${TIMEOUT:-180}"

NICE_LEVEL="${NICE_LEVEL:-10}"
JOB_TAG="${JOB_TAG:-${MODE}_${MARKET_TYPE}}"

usage() {
  cat <<'USAGE'
Usage: run_lowprio_optimize.sh [--mode parallel|refine] [--market-type 5m] [--seed-config PATH]
                               [--job-tag TAG] [--max-samples N] [--workers N]

Environment overrides:
  REPO_DIR OUT_ROOT ARCHIVE_ROOT BASE_URL
  MODE MARKET_TYPE SEED_CONFIG LOOKBACKS MAX_TRADES MAX_SAMPLES
  WORKERS GENERATIONS POPULATION MIN_TRADES TRADE_TARGET WIN_FLOOR SEED
  REFINE_ITERS RETRIES TIMEOUT NICE_LEVEL JOB_TAG
  REFINE_LOOKBACK_MINUTES REFINE_MAX_TRADES REFINE_MIN_TRADES REFINE_WIN_FLOOR
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="${2:-}"; shift 2 ;;
    --market-type) MARKET_TYPE="${2:-}"; shift 2 ;;
    --seed-config) SEED_CONFIG="${2:-}"; shift 2 ;;
    --job-tag) JOB_TAG="${2:-}"; shift 2 ;;
    --max-samples) MAX_SAMPLES="${2:-}"; shift 2 ;;
    --workers) WORKERS="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ "$MARKET_TYPE" != "5m" ]]; then
  echo "[lowprio-opt] only 5m is enabled now (got: $MARKET_TYPE)" >&2
  exit 2
fi
if [[ "$MODE" != "parallel" && "$MODE" != "refine" ]]; then
  echo "[lowprio-opt] invalid MODE=$MODE" >&2
  exit 2
fi

mkdir -p "$OUT_ROOT" "$ARCHIVE_ROOT"
ts="$(date -u +%Y%m%dT%H%M%SZ)"
job_id="${JOB_TAG}_${ts}"
log_file="$OUT_ROOT/${job_id}.log"
checkpoint_file="$OUT_ROOT/${job_id}.checkpoint.json"
result_file="$OUT_ROOT/${job_id}.result.json"
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

cmd=()
if [[ "$MODE" == "parallel" ]]; then
  cmd=(
    python3 -u scripts/forge/strategy_parallel_search.py
    --base-url "$BASE_URL"
    --market-type "$MARKET_TYPE"
    --seed-config "$SEED_CONFIG"
    --lookbacks "$LOOKBACKS"
    --max-trades "$MAX_TRADES"
    --max-samples "$MAX_SAMPLES"
    --generations "$GENERATIONS"
    --population "$POPULATION"
    --workers "$WORKERS"
    --min-trades "$MIN_TRADES"
    --trade-target "$TRADE_TARGET"
    --win-floor "$WIN_FLOOR"
    --timeout "$TIMEOUT"
    --retries "$RETRIES"
    --checkpoint "$checkpoint_file"
    --out "$result_file"
    --seed "$SEED"
  )
else
  cmd=(
    python3 -u scripts/forge/strategy_refine_search.py
    --base-url "$BASE_URL"
    --market-type "$MARKET_TYPE"
    --input "$SEED_CONFIG"
    --iters "$REFINE_ITERS"
    --lookback-minutes "$REFINE_LOOKBACK_MINUTES"
    --max-trades "$REFINE_MAX_TRADES"
    --min-trades "$REFINE_MIN_TRADES"
    --win-floor "$REFINE_WIN_FLOOR"
    --max-samples "$MAX_SAMPLES"
    --retries "$RETRIES"
    --out "$result_file"
  )
fi

{
  printf '{\n'
  printf '  "job_id": "%s",\n' "$job_id"
  printf '  "mode": "%s",\n' "$MODE"
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

# Async archiver (non-blocking): waits process exit, snapshots outputs into archive dir.
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

  python3 - <<PY >/dev/null 2>&1
import json
from pathlib import Path
meta = Path(r"$meta_file")
if meta.exists():
    data = json.loads(meta.read_text(encoding="utf-8"))
else:
    data = {}
data["ended_at_utc"] = r"$end_utc"
data["result_exists"] = Path(r"$result_file").exists()
data["checkpoint_exists"] = Path(r"$checkpoint_file").exists()
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
archive_dir: $ARCHIVE_ROOT
EOF
