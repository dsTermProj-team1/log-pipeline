package com.logpipeline.mapreduce;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.logpipeline.common.LogEvent;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

/**
 * JSONL 한 줄 -> LogEvent 파싱 -> (service\thour, {requests=1, 4xx, 5xx}) emit
 * 깨진 줄은 Counter로 집계만 하고 폐기 (잡 전체는 죽이지 않음)
 */
public class LogAggregationMapper extends Mapper<LongWritable, Text, Text, AggWritable> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Text outKey = new Text();
    private final AggWritable outVal = new AggWritable();

    enum BadRecord {MALFORMED_JSON, BAD_TIMESTAMP, MISSING_SERVICE}

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, AggWritable>.Context context) throws IOException, InterruptedException {

        String line = value.toString();
        if (line.isEmpty()) return;

        LogEvent event;
        try {
            event = objectMapper.readValue(line, LogEvent.class);
        } catch (Exception e) {
            context.getCounter(BadRecord.MALFORMED_JSON).increment(1);
            return;
        }

        String service = event.getService();
        if (service == null || service.isEmpty()) {
            context.getCounter(BadRecord.MISSING_SERVICE).increment(1);
            return;
        }

        int hour = extractHour(event.getTimestamp());
        if (hour < 0) {
            context.getCounter(BadRecord.BAD_TIMESTAMP).increment(1);
            return;
        }

        int status = event.getStatus();
        long is4xx = (status >= 400 && status < 500) ? 1L : 0L;
        long is5xx = (status >= 500 && status < 600) ? 1L : 0L;

        outKey.set(service + "\t" + hour);
        outVal.set(1L, is4xx, is5xx);
        context.write(outKey, outVal);
    }

    private int extractHour(String timestamp) {
        if (timestamp == null) return -1;
        try {
            return OffsetDateTime.parse(timestamp).getHour();
        } catch (Exception ignore) {
        }
        try {
            return LocalDateTime.parse(timestamp).getHour();
        } catch (Exception ignore) {
        }
        return -1;
    }
}
