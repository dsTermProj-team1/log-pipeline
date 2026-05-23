package com.logpipeline.kafka.hdfs;

import com.logpipeline.kafka.config.AppProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;


/**
 * HDFS 에 로그 라인을 쓰고, 다음 중 하나 만족 시 파일을 롤링한다.
 * 1) 파일 크기 >= rolling.max_bytes
 * 2) 현재 hour 디렉토리가 파일이 속한 hour와 다름
 * 3) 메세지 수 >= rolling.max-messages
 * <p>
 * 경로 규칙:
 * {basePath}/year=YYYY/month=MM/day=DD/hour=HH/part-{podId}-{uuid}.json
 * <p>
 * pod 여러 개가 같은 디렉토리에 동시 쓰기를 해도 파일명에 UUID가 들어가 충돌하지 않는다.
 */
@Component
@Slf4j
public class HdfsFileManager {
    private static final DateTimeFormatter PATH_FMT =
        DateTimeFormatter.ofPattern("'year='yyyy/'month='MM/'day='dd/'hour='HH");

    private final AppProperties props;
    private final String podId;

    private FileSystem fs;
    private FSDataOutputStream currentStream;
    private Path currentPath;
    private String currentHourDir; // 예: "year=2026/month=05/day=16/hour=14"
    private long currentBytes = 0L;
    private long currentMessages = 0L;

    public HdfsFileManager(AppProperties props) {
        this.props = props;
        // Pod hostname (예: kafka-consumer-adfadf-adfad)
        this.podId = System.getenv().getOrDefault("HOSTNAME", "local");
    }

    @PostConstruct
    public void init() {
        log.info("HDFS connected: uri={}, basePath={}, podId={}",
            props.getHdfs().getUri(), props.getHdfs().getBasePath(), podId);
    }

    private void ensureConnected() throws IOException {
        if (fs != null) return;
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", props.getHdfs().getUri());
        this.fs = FileSystem.get(URI.create(props.getHdfs().getUri()), conf);
        log.info("HDFS connected: {}", props.getHdfs().getUri());
    }

    /**
     * 한 줄 (JSON Line) 쓰기. 필요 시 자동 롤링
     * Kafka consumer 가 이 메서드 호출 후 성공하면 offset commit
     */
    public synchronized void writeLine(String jsonLine) throws IOException {
        ensureFileOpen();

        byte[] data = (jsonLine + "\n").getBytes(StandardCharsets.UTF_8);
        currentStream.write(data);
        currentStream.hflush();

        currentBytes += data.length;
        currentMessages++;

        if (shouldRoll()) {
            rollFile();
        }
    }

    /**
     * Spring 이 1분마다 호출 (스케줄러가 외부에서 호출)
     * 시간이 지나 hour 디렉토리가 바뀌었으면 빈 파일이라도 롤링하기 위함
     */
    public synchronized void checkTimeBasedRoll() {
        if (currentStream == null) return;
        if (!hourDirOf(now()).equals(currentHourDir)) {
            log.info("Time-based rolling triggered (hour changed)");
            try {
                rollFile();
            } catch (IOException e) {
                log.error("Failed to roll file on time check", e);
            }
        }
    }

    private boolean shouldRoll() {
        return currentBytes >= props.getRolling().getMaxBytes()
            || currentMessages >= props.getRolling().getMaxMessages()
            || !hourDirOf(now()).equals(currentHourDir);
    }

    private void ensureFileOpen() throws IOException {
        ensureConnected();
        if (currentStream != null) return;
        openNewFile();
    }

    private void rollFile() throws IOException {
        closeCurrentFile();
        openNewFile();
    }

    private void openNewFile() throws IOException {
        LocalDateTime now = LocalDateTime.now();
        this.currentHourDir = hourDirOf(now);
        String fileName = String.format("part-%s-%s.jsonl",
            podId, UUID.randomUUID().toString().substring(0, 8));
        this.currentPath = new Path(props.getHdfs().getBasePath() + "/" + currentHourDir + "/" + fileName);

        fs.mkdirs(currentPath.getParent());
        this.currentStream = fs.create(currentPath, true);
        this.currentBytes = 0;
        this.currentMessages = 0;

        log.info("Opened new HDFS file: {}", currentPath);
    }

    private void closeCurrentFile() throws IOException {
        if (currentStream == null) return;
        try {
            currentStream.close();
            log.info("Closed HDFS file: {} ({} bytes, {} messages",
                currentPath, currentBytes, currentMessages);
        } finally {
            currentStream = null;
            currentPath = null;
        }
    }

    @PreDestroy
    public void shutdown() throws IOException {
        log.info("Shutting down HDFS fileManager");
        closeCurrentFile();
        if (fs != null) fs.close();
    }

    private LocalDateTime now() {
        return LocalDateTime.now(ZoneOffset.UTC);
    }

    private String hourDirOf(LocalDateTime t) {
        return t.format(PATH_FMT);
    }
}
