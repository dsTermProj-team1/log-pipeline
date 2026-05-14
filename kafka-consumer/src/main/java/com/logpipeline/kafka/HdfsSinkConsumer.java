package com.logpipeline.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka → HDFS 싱크 컨슈머 (구현 예정).
 *
 * TODO:
 *  - KafkaConsumer 설정 (group.id, auto.offset.reset 등)
 *  - HDFS 파일을 시간 단위로 롤링 (예: /logs/msa-logs/yyyy=MM=dd/HH/part-*)
 *  - 배치 단위 flush + 정상 종료 시 commit
 */
public class HdfsSinkConsumer {

    private static final Logger log = LoggerFactory.getLogger(HdfsSinkConsumer.class);

    public static void main(String[] args) {
        log.info("HdfsSinkConsumer starting... (skeleton)");
        // TODO: implement
    }
}
