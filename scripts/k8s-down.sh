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

# ── 추가: Strimzi KafkaTopic finalizer 선제 제거 ──
# Topic Operator가 namespace와 함께 죽으면 finalizer를 못 풀어 교착되므로,
# 삭제 전에 미리 finalizer를 비운
echo "==> Strimzi KafkaTopic finalizer 정리..."
for t in $(kubectl get kafkatopic -n kafka -o name 2>/dev/null); do
  kubectl patch "$t" -n kafka -p '{"metadata":{"finalizers":null}}' --type=merge 2>/dev/null || true
done

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
