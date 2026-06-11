# mapreduce-job

HDFS 위의 로그를 시간대별 / 서비스별로 집계.

## 산출물
- 서비스별 요청 수
- 시간대별 4xx/5xx 에러 수
- 출력 두 갈래 (Reducer 가 한 잡에서 동시에 씀):
  - HDFS `/results/hourly/...` JSONL — baseline 잡(EWMA)의 입력 계약, 기존 포맷 그대로
  - Elasticsearch `msa-batch` 인덱스 — es-hadoop `EsOutputFormat` (MultipleOutputs).
    HDFS 문서에 `window_start`(시계열 필드), `doc_id`(`service@window`, 재실행 멱등 키)를 추가한 확장 문서

## 2차 집계 (baseline) → ES `msa-baseline`

`BaselineAggregationDriver`(1일 배치)는 1차 결과를 EWMA로 집계해 구간별 필요 Pod 수를
`/results/baseline/latest`(KEDA 소비)에 쓰고, **Kibana 시각화용으로 ES `msa-baseline`
인덱스에도 적재**한다. 최종 결과(4건)는 reducer가 아니라 드라이버가 직접 쓰므로
`EsOutputFormat` 대신 드라이버에서 ES `_bulk` REST 로 적재.
문서: `window`, `replicas`, `ref_date`, `computed_at`, `doc_id`(`ref_date@window`, 멱등).
ES 적재는 best-effort — 실패해도 HDFS 출력(오토스케일 핵심)이 정상이면 잡은 성공.

## ES 적재 설정 (-D 로 덮어쓰기)

| 키 | 기본값 | 설명 |
|----|--------|------|
| `agg.es.enabled` | `true` | 1차 잡: `false` 면 ES 적재 생략 (벤치마크 등) |
| `agg.window.start` | 입력 경로 `year=/month=/day=/hour=` 에서 유도 | 집계 윈도우 시작(ISO-8601) |
| `baseline.es.enabled` | `true` | 2차(baseline) 잡: `false` 면 ES 적재 생략 |
| `es.nodes` | `elasticsearch.monitoring.svc.cluster.local:9200` | msa-ott-service 가 띄운 ES (1차·2차 공통) |
| `es.resource` | `msa-batch` | 1차 대상 인덱스 |
| `baseline.es.resource` | `msa-baseline` | 2차 대상 인덱스 |

## 실행 (로컬)
```bash
../gradlew :mapreduce-job:build
hadoop jar build/libs/mapreduce-job-0.1.0-SNAPSHOT.jar \
  /logs/msa-logs/2026/05/14 /output/service-load
```

## 이미지 빌드 (잡 갱신 시)
```bash
../gradlew :mapreduce-job:build
docker build -t kevinpsj/mapreduce-job:0.3.1 .
docker push kevinpsj/mapreduce-job:0.3.1
```
