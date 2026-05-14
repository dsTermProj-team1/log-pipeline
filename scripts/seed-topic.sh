#!/usr/bin/env bash
# 토픽에 샘플 로그 한 줄 넣어보는 디버그용 스크립트
set -euo pipefail

MSG='{"timestamp":"2026-05-14T10:00:00Z","service":"streaming","method":"GET","uri":"/api/watch/123","status":200,"ip":"10.0.0.1","user_id":"u1","has_valid_token":true,"latency_ms":42}'

echo "$MSG" | docker compose exec -T kafka kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic msa-logs
