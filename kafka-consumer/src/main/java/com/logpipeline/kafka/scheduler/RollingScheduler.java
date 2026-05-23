package com.logpipeline.kafka.scheduler;

import com.logpipeline.kafka.hdfs.HdfsFileManager;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 트래픽이 없는 시간대에도 hour 가 바뀌면 파일을 롤링하기 위한 스케줄러
 * 메세지가 계속 들어오면 HdfsFileManager.writeLine()안에서 롤링되지만,
 * 메세지가 끊긴 동안 hour가 넘어가는 경우를 이 스케줄러가 처리함
 */
@Component
@RequiredArgsConstructor
public class RollingScheduler {
    private final HdfsFileManager fileManager;

    @Scheduled(fixedRateString = "${app.rolling.check-interval-ms}")
    public void checkRolling() {
        fileManager.checkTimeBasedRoll();
    }


}
