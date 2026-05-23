#!/usr/bin/env bash
# K3s에 모든 인프라 매니페스트를 순서대로 적용 (idempotent)
# 새 인프라가 추가되면 아래 블록 활성화
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> [1/N] Kafka"
kubectl apply -f k8s/kafka/namespace.yaml

if ! kubectl get deployment strimzi-cluster-operator -n kafka >/dev/null 2>&1; then
  echo "  Strimzi Operator 설치 중..."
   # create 대신 apply 사용 (이미 있는 리소스는 갱신, 없으면 생성 → AlreadyExists 안 남)
  kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
  kubectl wait --for=condition=Ready pod \
    -l name=strimzi-cluster-operator \
    -n kafka --timeout=300s
fi

kubectl apply -f k8s/kafka/kafka-cluster.yaml
echo "  Kafka 클러스터 Ready 대기..."
kubectl wait kafka/msa-kafka --for=condition=Ready --timeout=600s -n kafka

kubectl apply -f k8s/kafka/msa-logs-topic.yaml

# 인프라 추가될 때 아래 블록 활성화
echo "==> [2/N] Hadoop"
kubectl apply -f k8s/hadoop/namespace.yaml
kubectl apply -f k8s/hadoop/configmap.yaml
kubectl apply -f k8s/hadoop/namenode-service.yaml
kubectl apply -f k8s/hadoop/namenode-statefulset.yaml
kubectl wait --for=condition=Ready pod/hdfs-namenode-0 -n hadoop --timeout=300s
kubectl apply -f k8s/hadoop/datanode-service.yaml
kubectl apply -f k8s/hadoop/datanode-statefulset.yaml
kubectl wait --for=condition=Ready pod/hdfs-datanode-0 -n hadoop --timeout=300s
kubectl apply -f k8s/hadoop/init-job.yaml

# echo "==> [3/N] Elasticsearch"
# kubectl apply -f k8s/elasticsearch/

# echo "==> [4/N] KEDA"
# kubectl apply -f k8s/keda/

echo ""
echo "==> 완료. 상태:"
kubectl get pods -A | grep -vE "kube-system|kube-public|kube-node-lease"
