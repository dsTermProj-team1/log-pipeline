# ── Build stage ──
FROM eclipse-temurin:11-jdk AS build
WORKDIR /app

# Gradle wrapper + 빌드 스크립트 먼저 복사 (레이어 캐싱)
COPY gradlew settings.gradle build.gradle ./
COPY gradle ./gradle
COPY common ./common
COPY kafka-consumer ./kafka-consumer

# bootJar 빌드
RUN chmod +x gradlew
RUN ./gradlew :kafka-consumer:bootJar --no-daemon

# ── Runtime stage ──
FROM eclipse-temurin:11-jre
WORKDIR /app

# 빌드 결과 jar만 복사
COPY --from=build /app/kafka-consumer/build/libs/*.jar app.jar

# 비루트 사용자로 실행 (보안)
RUN useradd -r -u 1001 appuser
USER appuser

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
