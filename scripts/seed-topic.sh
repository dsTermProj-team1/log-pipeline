#!/usr/bin/env bash
# msa-logs 토픽에 샘플 메시지 발행 (디버그/검증용)
# 새 스키마 기준: timestamp, service, ip, method, uri, status, has_valid_token
#
# 사용 예:
#   ./scripts/seed-topic.sh             # 정상 메시지
#   ./scripts/seed-topic.sh auth-bypass # 인증 우회
set -euo pipefail

TYPE="${1:-normal}"

case "$TYPE" in
  normal)
    MSG='{"timestamp":"2026-05-16T14:23:45.123Z","service":"streaming","ip":"10.0.4.231","method":"GET","uri":"/api/streaming/play/movie_15","status":200,"has_valid_token":true}'
    ;;
  auth-bypass)
    MSG='{"timestamp":"2026-05-16T14:25:03.501Z","service":"streaming","ip":"10.0.7.114","method":"GET","uri":"/api/streaming/play/movie_15","status":401,"has_valid_token":false}'
    ;;
  *)
    echo "Usage: $0 [normal|auth-bypass]"
    exit 1
    ;;
esac

echo "$MSG" | kubectl -n kafka exec -i msa-kafka-combined-0 -- \
  bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic msa-logs

echo "[seed-topic] '$TYPE' 메시지 발행 완료"
