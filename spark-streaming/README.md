# spark-streaming

실시간 이상 탐지 엔진. **언어 미정** (PySpark 또는 Java/Scala).

## 결정 후 할 일

PySpark로 가는 경우:
- `pyproject.toml` 추가 (pyspark, elasticsearch)
- `src/anomaly_detector/main.py` 작성

Java/Scala로 가는 경우:
- 루트 `settings.gradle` 에 `include 'spark-streaming'` 추가
- `build.gradle` 작성 (spark-sql-kafka, elasticsearch-hadoop)
- `src/main/java/com/logpipeline/spark/AnomalyDetector.java` 작성

공통 로그 스키마는 `common/src/main/resources/schema/log-schema.json` 참고.
