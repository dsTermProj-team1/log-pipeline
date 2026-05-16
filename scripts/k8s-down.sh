#!/usr/bin/env bash
# K3s 인프라 정리 (namespace 단위 삭제 → 안의 모든 리소스/PVC 함께 삭제)
# namespace 삭제는 finalizer 처리 때문에 분 단위로 걸릴 수 있어, 직렬로 돌리면
# 한 namespace에서 막혔을 때 그 뒤가 영영 실행 안 됨 → 병렬로 띄우고 wait.
set -euo pipefail

NAMESPACES=(
  kafka
  hadoop
  # elasticsearch
  # keda
)

echo "==> 다음 namespace를 삭제합니다 (PVC 포함, 데이터 영구 손실):"
printf '  - %s\n' "${NAMESPACES[@]}"
read -p "계속하려면 'yes' 입력: " confirm
if [ "$confirm" != "yes" ]; then
  echo "취소됨"
  exit 1
fi

pids=()
for ns in "${NAMESPACES[@]}"; do
  kubectl delete namespace "$ns" --ignore-not-found &
  pids+=($!)
done

for pid in "${pids[@]}"; do
  wait "$pid" || true
done

echo ""
echo "==> 남은 namespace:"
kubectl get namespaces
