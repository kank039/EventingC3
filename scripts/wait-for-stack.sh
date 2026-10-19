#!/usr/bin/env bash
# Waits for the docker-compose ClickHouse and Redpanda services to accept connections.
set -euo pipefail
for i in $(seq 1 60); do
  if curl -sf http://localhost:8123/ping >/dev/null && docker compose exec -T redpanda rpk cluster health --exit-when-healthy >/dev/null 2>&1; then
    echo "stack ready"; exit 0
  fi
  sleep 2
done
echo "stack not ready" >&2; exit 1
