#!/usr/bin/env bash
# K3s 인프라 정리 (namespace 단위 삭제 → 안의 모든 리소스/PVC 함께 삭제)
set -euo pipefail

echo "==> 모든 인프라 namespace를 삭제합니다 (PVC 포함, 데이터 영구 손실)"
read -p "계속하려면 'yes' 입력: " confirm
if [ "$confirm" != "yes" ]; then
  echo "취소됨"
  exit 1
fi

kubectl delete namespace kafka --ignore-not-found
kubectl delete namespace hadoop --ignore-not-found
# kubectl delete namespace elasticsearch --ignore-not-found
# kubectl delete namespace keda --ignore-not-found

echo ""
echo "==> 남은 namespace:"
kubectl get namespaces
