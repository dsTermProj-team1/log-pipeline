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

### 사전 조건
- K3s 클러스터 (WSL2 권장)
- kubectl 설치 및 K3s kubeconfig 연결
- 레포 루트에 `.env` 파일 준비 (`.env.example` 복사 후 값 채우기)

### 인프라 배포
```bash
./scripts/k8s-up.sh
```

### 토픽 동작 확인 (샘플 메시지 발행)
```bash
./scripts/seed-topic.sh             # 정상 트래픽
./scripts/seed-topic.sh auth-bypass # 인증 우회 시도
```

### 상태 확인
```bash
./scripts/k8s-status.sh
```

### Java 모듈 빌드
```bash
./gradlew build
```

### 인프라 정리
```bash
./scripts/k8s-down.sh
```

## 관련 레포

- `ott-msa` — FastAPI 기반 OTT MSA + 트래픽 시뮬레이터
