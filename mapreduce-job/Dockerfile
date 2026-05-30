FROM apache/hadoop:3.3.6

# 호스트에서 빌드한 uber jar 만 복사 (kafka-consumer 와 동일 전략: 멀티스테이지 X)
# 빌드 컨텍스트가 mapreduce-job/ 이라 build/libs 경로가 상대경로로 잡힘
COPY build/libs/mapreduce-job-0.1.0-SNAPSHOT.jar /opt/job/mapreduce-job.jar

# 잡 제출 후 완료까지 대기하는 게 목적 → 상시 데몬 아님.
# 실제 제출 커맨드는 CronJob 에서 주입(이미지엔 jar 만 둠).