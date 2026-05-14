# log-pipeline

MSA 로그 기반 이상 탐지 시스템의 로그 처리 파이프라인 레포.
MSA 서비스(`ott-msa`)에서 발생하는 로그를 Kafka로 수집해
Batch(Hadoop MapReduce)와 Speed(Spark Streaming) 양쪽으로 처리하고
Elasticsearch로 적재합니다.

## 모듈 구성

| 모듈 | 언어 | 역할 |
|------|------|------|
| `common` | Java 11 | 공통 로그 스키마 / 유틸 |
| `kafka-consumer` | Java 11 | Kafka → HDFS 적재 |
| `mapreduce-job` | Java 11 | HDFS 위 배치 분석 |
| `spark-streaming` | TBD | 실시간 이상 탐지 |

## 빠른 시작

```bash
# 1. 로컬 인프라 기동 (Kafka, HDFS, ES, Kibana)
./scripts/dev-up.sh

# 2. Java 모듈 빌드
./gradlew build

# 3. 종료
./scripts/dev-down.sh
```

## 관련 레포

- `ott-msa` — FastAPI 기반 OTT MSA + 트래픽 시뮬레이터
