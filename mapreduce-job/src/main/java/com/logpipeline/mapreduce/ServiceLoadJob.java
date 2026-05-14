package com.logpipeline.mapreduce;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 서비스별 / 시간대별 부하 집계 MapReduce 잡 (구현 예정).
 *
 * TODO:
 *  - Mapper: 로그 라인 파싱 → (service|hour, 1) 출력
 *  - Reducer: 합산
 *  - 결과를 HDFS 출력 후 별도 단계에서 ES 적재
 */
public class ServiceLoadJob {

    private static final Logger log = LoggerFactory.getLogger(ServiceLoadJob.class);

    public static void main(String[] args) throws Exception {
        log.info("ServiceLoadJob starting... (skeleton)");
        // TODO: implement
    }
}
