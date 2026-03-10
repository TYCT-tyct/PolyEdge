#!/usr/bin/env bash
set -euo pipefail

# Forge-only disk guard for Tokyo relay hosts.
# This is intentionally conservative: keep runtime binaries, prune build/cache artifacts.

ROOT_USAGE_PCT="${ROOT_USAGE_PCT:-85}"
JOURNAL_KEEP="${JOURNAL_KEEP:-200M}"
REPO_DIR="${REPO_DIR:-/home/ubuntu/PolyEdge}"
HOME_DIR="${HOME_DIR:-/home/ubuntu}"
RELEASE_ROOT="${RELEASE_ROOT:-$HOME_DIR/polyedge-releases}"
RUSTUP_HOME_DIR="${RUSTUP_HOME_DIR:-/home/ubuntu/.rustup}"
CARGO_HOME_DIR="${CARGO_HOME_DIR:-/home/ubuntu/.cargo}"

log() {
  printf '[tokyo-disk-guard] %s\n' "$*"
}

usage_pct() {
  df -P / | awk 'NR==2 {gsub("%","",$5); print $5+0}'
}

active_release_prefix() {
  local env_line commit
  env_line="$(systemctl show polyedge-forge-tokyo.service -p Environment --value 2>/dev/null || true)"
  commit="$(printf '%s\n' "$env_line" | tr ' ' '\n' | sed -n 's/^POLYEDGE_RELEASE_COMMIT=//p' | head -n1)"
  if [[ -n "$commit" ]]; then
    printf '%s-' "${commit:0:7}"
  fi
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

prune_releases() {
  local keep_prefix="$1"
  [[ -d "$RELEASE_ROOT" ]] || return
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
  done < <(find "$RELEASE_ROOT" -mindepth 1 -maxdepth 1 -type d | sort)
}

prune_target_release() {
  local rel="$REPO_DIR/target/release"
  if [[ ! -d "$rel" ]]; then
    return
  fi
  log "prune target/release build artifacts"
  rm -rf \
    "$rel/deps" \
    "$rel/build" \
    "$rel/.fingerprint" \
    "$rel/incremental" \
    "$rel/examples"
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
  log "apt clean"
  apt-get clean >/dev/null 2>&1 || true
  rm -rf /var/tmp/* /tmp/* 2>/dev/null || true
  rm -rf "$HOME_DIR/polyedge_dirty_backup" 2>/dev/null || true
  prune_glob \
    "$HOME_DIR/PolyEdge_dirty_*" \
    "$HOME_DIR/"'polyedge_*.bundle' \
    "$HOME_DIR/"'*.tgz'
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
  prune_target_release
  prune_cargo_cache
  prune_releases "$active"

  # Emergency branch: rustup toolchains can dominate small root volumes.
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
