#!/usr/bin/env bash
set -euo pipefail

# Deploy the latest GHCR Airflow image to local docker-compose services.
# Usage:
#   ./scripts/deploy_airflow_from_ghcr.sh
#
# Optional env vars:
#   COMPOSE_FILE_PATH (default: docker-compose.yml)
#   ENV_FILE_PATH (default: .env)
#   AIRFLOW_IMAGE (default: ghcr.io/romainr99/anidata-lab-airflow:latest)

COMPOSE_FILE_PATH="${COMPOSE_FILE_PATH:-docker-compose.yml}"
ENV_FILE_PATH="${ENV_FILE_PATH:-.env}"
AIRFLOW_IMAGE="${AIRFLOW_IMAGE:-ghcr.io/romainr99/anidata-lab-airflow:latest}"

echo "==> Pull image from GHCR: ${AIRFLOW_IMAGE}"
/usr/local/bin/docker pull "${AIRFLOW_IMAGE}"

echo "==> Restart platform services with docker compose"
/usr/local/bin/docker compose -f "${COMPOSE_FILE_PATH}" --env-file "${ENV_FILE_PATH}" down
/usr/local/bin/docker compose -f "${COMPOSE_FILE_PATH}" --env-file "${ENV_FILE_PATH}" up -d postgres elasticsearch grafana airflow-init airflow-webserver airflow-scheduler

echo "==> Current service status"
/usr/local/bin/docker compose -f "${COMPOSE_FILE_PATH}" --env-file "${ENV_FILE_PATH}" ps

echo "==> Waiting for Airflow webserver and Elasticsearch health"
for i in {1..30}; do
  airflow_status="$(/usr/local/bin/docker compose -f "${COMPOSE_FILE_PATH}" --env-file "${ENV_FILE_PATH}" ps airflow-webserver --format json 2>/dev/null || true)"
  es_status="$(/usr/local/bin/docker compose -f "${COMPOSE_FILE_PATH}" --env-file "${ENV_FILE_PATH}" ps elasticsearch --format json 2>/dev/null || true)"
  grafana_status="$(/usr/local/bin/docker compose -f "${COMPOSE_FILE_PATH}" --env-file "${ENV_FILE_PATH}" ps grafana --format json 2>/dev/null || true)"
  if [[ "${airflow_status}" == *"healthy"* ]] && [[ "${es_status}" == *"healthy"* ]] && [[ "${grafana_status}" == *"running"* ]]; then
    echo "Airflow webserver and Elasticsearch are healthy, Grafana is running."
    exit 0
  fi
  sleep 5
done

echo "Platform did not become healthy within timeout."
exit 1
