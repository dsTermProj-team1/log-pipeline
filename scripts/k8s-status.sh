#!/usr/bin/env bash
# 인프라 현재 상태 한눈에 보기
set -euo pipefail

echo "==> Pods (시스템 제외)"
kubectl get pods -A | grep -vE "kube-system|kube-public|kube-node-lease" || true
echo ""

echo "==> Kafka 리소스"
kubectl get kafka,kafkanodepool,kafkatopic -n kafka 2>/dev/null || echo "  Kafka 미배포"
echo ""

echo "==> KEDA 스케일링"
kubectl get scaledobject,hpa -n kafka 2>/dev/null || echo "  ScaledObject 미배포"
echo ""

echo "==> CronJobs (배치 + baseline + updater)"
kubectl get cronjob -A 2>/dev/null \
  | grep -E "NAMESPACE|mapreduce-hourly-agg|baseline-aggregation|keda-updater" \
  || echo "  CronJob 미배포"
echo ""

echo "==> PVC"
kubectl get pvc -A 2>/dev/null | grep -v "kube-system" || true