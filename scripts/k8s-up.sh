#!/usr/bin/env bash
# K3s에 모든 인프라 매니페스트를 순서대로 적용 (idempotent)
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> [1/6] Kafka"
kubectl apply -f k8s/kafka/namespace.yaml

if ! kubectl get deployment strimzi-cluster-operator -n kafka >/dev/null 2>&1; then
  echo "  Strimzi Operator 설치 중..."
  kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
  kubectl wait --for=condition=Ready pod \
    -l name=strimzi-cluster-operator \
    -n kafka --timeout=300s
fi

kubectl apply -f k8s/kafka/kafka-cluster.yaml
echo "  Kafka 클러스터 Ready 대기..."
kubectl wait kafka/msa-kafka --for=condition=Ready --timeout=600s -n kafka
kubectl apply -f k8s/kafka/msa-logs-topic.yaml
kubectl apply -f k8s/kafka/msa-ip-counts-topic.yaml

echo "==> [2/6] Hadoop (HDFS)"
kubectl apply -f k8s/hadoop/namespace.yaml
kubectl apply -f k8s/hadoop/configmap.yaml
kubectl apply -f k8s/hadoop/namenode-service.yaml
kubectl apply -f k8s/hadoop/namenode-statefulset.yaml
kubectl wait --for=condition=Ready pod/hdfs-namenode-0 -n hadoop --timeout=300s
kubectl apply -f k8s/hadoop/datanode-service.yaml
kubectl apply -f k8s/hadoop/datanode-statefulset.yaml
kubectl wait --for=condition=Ready pod/hdfs-datanode-0 -n hadoop --timeout=300s
kubectl apply -f k8s/hadoop/init-job.yaml

echo "==> [3/6] YARN"
kubectl apply -f k8s/hadoop/yarn/yarn-config.yaml          # ConfigMap 먼저 (없으면 RM 부팅 실패)
kubectl apply -f k8s/hadoop/yarn/resourcemanager.yaml
kubectl wait --for=condition=Ready pod -l app=yarn-resourcemanager -n hadoop --timeout=300s || true
kubectl apply -f k8s/hadoop/yarn/nodemanager.yaml
kubectl apply -f k8s/hadoop/yarn/jobhistoryserver.yaml
kubectl exec -n hadoop deploy/yarn-resourcemanager -- yarn node -list 2>/dev/null || true

echo "==> [4/6] MapReduce CronJobs (1차 집계 + baseline)"
kubectl apply -f k8s/hadoop/mapreduce/cronjob.yaml
kubectl apply -f k8s/hadoop/mapreduce/baseline-cronjob.yaml

echo "==> [5/6] KEDA (Helm)"
if ! kubectl get deployment keda-operator -n keda >/dev/null 2>&1; then
  echo "  KEDA 설치 중..."
  helm repo add kedacore https://kedacore.github.io/charts
  helm repo update
  # upgrade --install: 있으면 업그레이드, 없으면 설치 (idempotent)
  helm upgrade --install keda kedacore/keda --namespace keda --create-namespace
  kubectl wait --for=condition=Ready pod -l app=keda-operator -n keda --timeout=300s
fi

echo "==> [6/6] Speed consumer 스케일링 (ScaledObject + updater)"
# updater가 kafka ns에서 hadoop-conf 를 마운트 → hadoop ns에서 복제
kubectl get configmap hadoop-conf -n hadoop -o yaml \
  | sed 's/namespace: hadoop/namespace: kafka/' \
  | kubectl apply -f -
kubectl apply -f k8s/keda/speed-consumer-scaledobject.yaml
kubectl apply -f k8s/keda/updater-rbac.yaml
kubectl apply -f k8s/keda/updater-script-configmap.yaml
kubectl apply -f k8s/keda/updater-cronjob.yaml

echo ""
echo "==> 완료. 상태:"
kubectl get pods -A | grep -vE "kube-system|kube-public|kube-node-lease"