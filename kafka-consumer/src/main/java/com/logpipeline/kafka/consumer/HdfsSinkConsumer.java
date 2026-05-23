package com.logpipeline.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.logpipeline.common.LogEvent;
import com.logpipeline.kafka.hdfs.HdfsFileManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Kafka msa-logs 토픽을 구독하여 HDFS에 적재함
 *
 * 흐름:
 *  1) 메세지 수신 (raw JSON String)
 *  2) LogEvent 로 역직렬화 + 필수 필드 검증
 *  3) 검증 통과 시 HDFS 에 한 줄 쓰기
 *  4) 쓰기 성공 후 수동 offset commit (at-least-once)
 *
 * 검증 실패 메세지는 WARN 로그 후 폐기하고 offset은 commit (무한 재시도 방지)
 */

@Component
@Slf4j
@RequiredArgsConstructor
public class HdfsSinkConsumer {
    private final HdfsFileManager fileManager;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${app.kafka.topic}")
    public void consume(String rawMessage, Acknowledgment ack) {
        // 역직렬화 + 검증
        LogEvent event;
        try {
            event = objectMapper.readValue(rawMessage, LogEvent.class);
        } catch (Exception e) {
            log.warn("Malformed JSON, discarding: {}", truncate(rawMessage), e);
            ack.acknowledge(); // commit
            return;
        }

        if(!isValid(event)){
            log.warn("Validation failed, discarding: {}", truncate(rawMessage));
            ack.acknowledge();
            return;
        }

        try {
            fileManager.writeLine(rawMessage); // 원본 json  그대로 저장
            ack.acknowledge(); // 성공 시에만 commit
        } catch (IOException e) {
            log.error("HDFS write failed, will retry: {} ", truncate(rawMessage), e);
            throw new RuntimeException("HDFS write failed", e);
        }
    }

    /**
     * 필수 7개 필드 검증. 하나라도 누락/비정상이면 false
     * 리얼리티용 필드 (user_id 등)는 검증하지 않음 (nullable)
     */
    private boolean isValid(LogEvent e) {
         if (isBlank(e.getTimestamp())) return false;
        if (isBlank(e.getService())) return false;
        if (isBlank(e.getIp())) return false;
        if (isBlank(e.getMethod())) return false;
        if (isBlank(e.getUri())) return false;
        if (e.getStatus() < 100 || e.getStatus() > 599) return false;
        // hasValidToken은 누락 시 false로 들어옴 -> 검증 불필요
        return true;
    }

    private boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private String truncate(String s) {
        if(s == null) return null;
        return s.length() > 200 ? s.substring(0, 200) + "..." : s;
    }

}
