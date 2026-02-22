#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="/etc/polyedge/data-backend-ireland.env"
API_ENV_FILE="/etc/polyedge/data-backend-api.env"

install_redis() {
  if command -v redis-server >/dev/null 2>&1; then
    echo "[ok] redis-server already installed"
    return
  fi
  echo "[install] redis-server"
  sudo apt-get update -y
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y redis-server
}

install_clickhouse() {
  if command -v clickhouse-client >/dev/null 2>&1; then
    echo "[ok] clickhouse already installed"
    return
  fi
  echo "[install] clickhouse-server/client"
  sudo apt-get update -y
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y apt-transport-https ca-certificates dirmngr gnupg
  if [[ ! -f /etc/apt/sources.list.d/clickhouse.list ]]; then
    sudo mkdir -p /etc/apt/keyrings
    curl -fsSL https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key | sudo gpg --dearmor -o /etc/apt/keyrings/clickhouse.gpg
    echo "deb [signed-by=/etc/apt/keyrings/clickhouse.gpg] https://packages.clickhouse.com/deb stable main" | sudo tee /etc/apt/sources.list.d/clickhouse.list >/dev/null
  fi
  sudo apt-get update -y
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y clickhouse-server clickhouse-client
}

ensure_env_line() {
  local file="$1"
  local key="$2"
  local val="$3"
  sudo touch "$file"
  if sudo grep -q "^${key}=" "$file"; then
    sudo sed -i "s|^${key}=.*|${key}=${val}|g" "$file"
  else
    echo "${key}=${val}" | sudo tee -a "$file" >/dev/null
  fi
}

main() {
  install_redis
  install_clickhouse

  sudo systemctl enable --now redis-server
  sudo systemctl enable --now clickhouse-server

  ensure_env_line "$ENV_FILE" "POLYEDGE_CH_URL" "http://127.0.0.1:8123"
  ensure_env_line "$ENV_FILE" "POLYEDGE_CH_DATABASE" "polyedge"
  ensure_env_line "$ENV_FILE" "POLYEDGE_CH_SNAPSHOT_TABLE" "snapshot_1s"
  ensure_env_line "$ENV_FILE" "POLYEDGE_CH_TTL_DAYS" "30"
  ensure_env_line "$ENV_FILE" "POLYEDGE_REDIS_URL" "redis://127.0.0.1:6379/0"
  ensure_env_line "$ENV_FILE" "POLYEDGE_REDIS_PREFIX" "polyedge"
  ensure_env_line "$ENV_FILE" "POLYEDGE_REDIS_TTL_SEC" "7200"
  ensure_env_line "$ENV_FILE" "POLYEDGE_SINK_BATCH_SIZE" "200"
  ensure_env_line "$ENV_FILE" "POLYEDGE_SINK_FLUSH_MS" "1000"
  ensure_env_line "$ENV_FILE" "POLYEDGE_SINK_QUEUE_CAP" "20000"

  ensure_env_line "$API_ENV_FILE" "POLYEDGE_REDIS_URL" "redis://127.0.0.1:6379/0"
  ensure_env_line "$API_ENV_FILE" "POLYEDGE_REDIS_PREFIX" "polyedge"

  sudo systemctl daemon-reload
  sudo systemctl restart polyedge-data-backend-ireland.service polyedge-data-backend-api.service

  echo "[status] redis"
  sudo systemctl --no-pager --full status redis-server | sed -n '1,12p'
  echo
  echo "[status] clickhouse"
  sudo systemctl --no-pager --full status clickhouse-server | sed -n '1,12p'
  echo
  echo "[status] ireland ingest/api"
  sudo systemctl --no-pager --full status polyedge-data-backend-ireland.service | sed -n '1,12p'
  sudo systemctl --no-pager --full status polyedge-data-backend-api.service | sed -n '1,12p'
}

main "$@"
