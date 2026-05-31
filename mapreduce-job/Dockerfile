FROM apache/hadoop:3.3.6

# 호스트에서 빌드한 uber jar 만 복사 (kafka-consumer 와 동일 전략: 멀티스테이지 X)
# 빌드 컨텍스트가 mapreduce-job/ 이라 build/libs 경로가 상대경로로 잡힘
COPY build/libs/mapreduce-job-0.1.0-SNAPSHOT.jar /opt/job/mapreduce-job.jar

# ── updater용: jq + kubectl 추가 ──
USER root
RUN curl -fsSLo /usr/local/bin/jq \
      "https://github.com/jqlang/jq/releases/download/jq-1.7.1/jq-linux-amd64" \
    && curl -fsSLo /usr/local/bin/kubectl \
      "https://dl.k8s.io/release/v1.30.0/bin/linux/amd64/kubectl" \
    && chmod +x /usr/local/bin/jq /usr/local/bin/kubectl
USER hadoop