# mapreduce-job

HDFS 위의 로그를 시간대별 / 서비스별로 집계.

## 산출물
- 서비스별 요청 수
- 시간대별 4xx/5xx 에러 수
- Elasticsearch `msa-batch` 인덱스로 적재

## 실행 (로컬)
```bash
../gradlew :mapreduce-job:build
hadoop jar build/libs/mapreduce-job-0.1.0-SNAPSHOT.jar \
  /logs/msa-logs/2026/05/14 /output/service-load
```
