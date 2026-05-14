#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[dev-up] starting infra..."
docker compose up -d

echo "[dev-up] waiting for kafka..."
until docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  sleep 2
done

echo "[dev-up] creating topic msa-logs (6 partitions)..."
docker compose exec -T kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic msa-logs \
  --partitions 6 \
  --replication-factor 1

echo "[dev-up] done"
echo "  Kafka     : localhost:9094"
echo "  HDFS UI   : http://localhost:9870"
echo "  ES        : http://localhost:9200"
echo "  Kibana    : http://localhost:5601"
