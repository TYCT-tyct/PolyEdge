# scripts/forge/ — OPS SCRIPTS

## OVERVIEW
Deployment, maintenance, and live execution profile management for Forge on Ireland + Tokyo.

## FILES
| Script | Purpose |
|--------|---------|
| `live_execution_profiles.env` | **COMMON + SHADOW/CANARY/FULL** env blocks — source before running live |
| `setup_ireland_forge.sh` | First-time Ireland server setup (deps, ClickHouse, Redis, systemd) |
| `setup_tokyo_forge.sh` | First-time Tokyo server setup |
| `tokyo_disk_guard.sh` | Disk-space guardian — kills relay if disk > threshold |
| `live_roundtrip_once.sh` | One-shot live order submit test (SHADOW profile) |
| `hpo_watch_jobs.ps1` | PowerShell: poll HPO job status from Windows dev machine |
| `profile_compare_remote.py` | Compare two strategy profile JSON files, score delta |
| `rebuild_probability_columns.py` | Backfill ClickHouse probability columns after schema change |
| `recompute_full_history.py` | Full history reprocessing (rounds/snapshots) |
| `repair_targets_backfill.py` | Backfill missing target prices for rounds |
| `reset_collection_data.py` | Wipe collector data for symbol/TF (use with caution) |

## LIVE PROFILE USAGE
```bash
# Load COMMON + CANARY, then run
source scripts/forge/live_execution_profiles.env
export $(grep -v '^#' live_execution_profiles.env | grep COMMON)
export $(grep -v '^#' live_execution_profiles.env | grep CANARY)
cargo run -p polyedge_forge -- ireland-api
```

## ROLLOUT ORDER
SHADOW → CANARY → FULL (never skip steps)

## ANTI-PATTERNS
- Don't run `reset_collection_data.py` on production without backup
- Don't modify `live_execution_profiles.env` directly on server — commit first
- Don't skip SHADOW phase when testing new strategy params
