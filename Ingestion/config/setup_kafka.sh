#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Setting up ingestion workspace..."

if [[ ! -f "${ROOT_DIR}/requirements.txt" ]]; then
  echo "requirements.txt not found"
  exit 1
fi

python -m pip install -r "${ROOT_DIR}/requirements.txt"

echo "Starting Kafka stack..."
docker compose -f "${ROOT_DIR}/docker-compose.yml" up -d

echo "Creating ingestion topics..."
python "${ROOT_DIR}/scripts/create_kafka_topics.py"

echo "Done."
