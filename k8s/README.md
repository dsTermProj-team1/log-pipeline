# k8s

K3s 배포 매니페스트. Phase 2 이후에 채울 예정.

## 디렉토리
- `kafka/` — Strimzi 또는 Bitnami Kafka 매니페스트
- `hadoop/` — HDFS namenode/datanode StatefulSet
- `spark/` — Spark on K8s (또는 SparkOperator)
- `elasticsearch/` — ECK 또는 Bitnami ES
- `keda/` — ScaledObject (Kafka lag + Cron 트리거)
