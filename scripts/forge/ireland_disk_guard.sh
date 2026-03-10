#!/usr/bin/env bash
set -euo pipefail

# Disk guard for Ireland API/recorder hosts.
# Keep live data under /data intact, prune only reproducible build and deploy artifacts.

ROOT_USAGE_PCT="${ROOT_USAGE_PCT:-85}"
JOURNAL_KEEP="${JOURNAL_KEEP:-300M}"
HOME_DIR="${HOME_DIR:-/home/ubuntu}"
REPO_DIR="${REPO_DIR:-$HOME_DIR/PolyEdge}"
DATA_RELEASE_ROOT="${DATA_RELEASE_ROOT:-/data/polyedge-releases}"
HOME_RELEASE_ROOT="${HOME_RELEASE_ROOT:-$HOME_DIR/polyedge-releases}"
CARGO_HOME_DIR="${CARGO_HOME_DIR:-$HOME_DIR/.cargo}"
RUSTUP_HOME_DIR="${RUSTUP_HOME_DIR:-$HOME_DIR/.rustup}"

log() {
  printf '[ireland-disk-guard] %s\n' "$*"
}

usage_pct() {
  df -P / | awk 'NR==2 {gsub("%","",$5); print $5+0}'
}

active_release_prefix() {
  local env_line commit
  env_line="$(systemctl show polyedge-forge-ireland-api.service -p Environment --value 2>/dev/null || true)"
  commit="$(printf '%s\n' "$env_line" | tr ' ' '\n' | sed -n 's/^POLYEDGE_RELEASE_COMMIT=//p' | head -n1)"
  if [[ -n "$commit" ]]; then
    printf '%s-' "${commit:0:7}"
  fi
}

prune_release_root() {
  local root="$1"
  local keep_prefix="$2"
  [[ -d "$root" ]] || return
  while IFS= read -r dir; do
    [[ -d "$dir" ]] || continue
    local base
    base="$(basename "$dir")"
    if [[ -n "$keep_prefix" && "$base" == "$keep_prefix"* ]]; then
      log "trim active release source tree: $dir"
      rm -rf "$dir/src"
      continue
    fi
    log "remove stale release: $dir"
    rm -rf "$dir"
  done < <(find "$root" -mindepth 1 -maxdepth 1 -type d | sort)
}

prune_glob() {
  local pattern
  for pattern in "$@"; do
    while IFS= read -r path; do
      [[ -e "$path" ]] || continue
      log "remove stale artifact: $path"
      rm -rf "$path"
    done < <(compgen -G "$pattern" || true)
  done
}

prune_build_artifacts() {
  if [[ -d "$REPO_DIR/target" ]]; then
    log "prune repo target dir"
    rm -rf "$REPO_DIR/target"
  fi
  if [[ -d "$HOME_DIR/.npm" ]]; then
    log "prune home npm cache"
    rm -rf "$HOME_DIR/.npm/_cacache" "$HOME_DIR/.npm/_logs" 2>/dev/null || true
  fi
}

prune_cargo_cache() {
  if [[ -d "$CARGO_HOME_DIR/registry/cache" ]]; then
    log "prune cargo registry cache"
    find "$CARGO_HOME_DIR/registry/cache" -type f -name '*.crate' -mtime +14 -delete || true
  fi
  if [[ -d "$CARGO_HOME_DIR/git/checkouts" ]]; then
    log "prune cargo git checkouts older than 14 days"
    find "$CARGO_HOME_DIR/git/checkouts" -mindepth 1 -maxdepth 1 -mtime +14 -exec rm -rf {} + || true
  fi
}

prune_misc() {
  log "vacuum journal to $JOURNAL_KEEP"
  journalctl --vacuum-size="$JOURNAL_KEEP" >/dev/null 2>&1 || true
  if [[ -d /var/log/clickhouse-server ]]; then
    log "truncate active clickhouse logs"
    truncate -s 0 /var/log/clickhouse-server/clickhouse-server.log 2>/dev/null || true
    truncate -s 0 /var/log/clickhouse-server/clickhouse-server.err.log 2>/dev/null || true
    log "prune old clickhouse archived logs"
    find /var/log/clickhouse-server -type f -name '*.gz' -mtime +2 -delete || true
  fi
  log "prune stale clickhouse backups"
  find /var/lib -maxdepth 1 -type d -name 'clickhouse.bak.*' -mtime +3 -exec rm -rf {} + || true
  log "apt clean"
  apt-get clean >/dev/null 2>&1 || true
  rm -rf /var/tmp/* /tmp/* 2>/dev/null || true
}

main() {
  local before after active
  before="$(usage_pct)"
  active="$(active_release_prefix)"
  log "root usage before=${before}% threshold=${ROOT_USAGE_PCT}% active_release=${active:-none}"

  if (( before < ROOT_USAGE_PCT )); then
    log "usage below threshold, skip"
    exit 0
  fi

  prune_misc
  prune_build_artifacts
  prune_cargo_cache
  prune_release_root "$DATA_RELEASE_ROOT" "$active"
  prune_release_root "$HOME_RELEASE_ROOT" "$active"
  prune_glob \
    "$HOME_DIR/PolyEdge_dirty_*" \
    "$HOME_DIR/PolyEdge_deploy*" \
    "$HOME_DIR/PolyEdge-[0-9a-f]*" \
    "$HOME_DIR/releases" \
    "$HOME_DIR/"'polyedge_*.bundle' \
    "$HOME_DIR/"'*.tgz'

  after="$(usage_pct)"
  if (( after >= ROOT_USAGE_PCT )) && [[ -d "$RUSTUP_HOME_DIR/toolchains" ]]; then
    log "usage still high (${after}%), prune old rustup toolchains except stable"
    find "$RUSTUP_HOME_DIR/toolchains" -mindepth 1 -maxdepth 1 -type d \
      ! -name 'stable-*' -exec rm -rf {} + || true
  fi

  after="$(usage_pct)"
  log "root usage after=${after}%"
}

main "$@"
