# kafka-consumer

Kafka topic `msa-logs` 를 구독해서 HDFS `/logs/msa-logs/` 에 적재.
Consumer Group ID 를 동일하게 두고 Pod 여러 개로 띄우면 파티션 자동 분배.

## 빌드

```bash
../gradlew :kafka-consumer:build
```
