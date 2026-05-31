package com.logpipeline.mapreduce.baseline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public class HourEwmaMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private final ObjectMapper json = new ObjectMapper();
    private final IntWritable outKey = new IntWritable();
    private final Text outVal = new Text();

    private LocalDate refDate;
    private int lookback;

    private String cachedDir = null;
    private long cachedDist = 0;
    private boolean cachedSkip = false;


    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        refDate = LocalDate.parse(conf.get("baseline.ref.date"));
        lookback = conf.getInt("baseline.lookback", 7);
    }

    /**
     * 현재 레코드가 속한 파일의 디렉토리명에서 dist 계산(파일 단위 캐시)
     */
    private void resolveDir(Context ctx) {
        String path = ctx.getConfiguration().get("map.input.file");
        if (path == null) {
            path = ctx.getConfiguration().get("mapreduce.map.input.file");
        }
        String dirName = (path == null) ? null
                : new org.apache.hadoop.fs.Path(path).getParent().getName();
        if (dirName != null && dirName.equals(cachedDir)) {
            return;   // 같은 파일 → 캐시 사용
        }
        cachedDir = dirName;
        try {
            LocalDate recDate = LocalDate.parse(dirName.substring(0, 10));
            cachedDist = ChronoUnit.DAYS.between(recDate, refDate);
            cachedSkip = (cachedDist < 0 || cachedDist >= lookback);
        } catch (Exception e) {
            cachedSkip = true;
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        resolveDir(ctx);
        if (cachedSkip) {
            return;
        }

        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }
        JsonNode node;
        try {
            node = json.readTree(line);
        } catch (Exception e) {
            ctx.getCounter("baseline", "bad_json").increment(1);
            return;
        }
        if (node.get("hour") == null || node.get("requests") == null) {
            ctx.getCounter("baseline", "missing_field").increment(1);
            return;
        }
        int hour = node.get("hour").asInt();
        long requests = node.get("requests").asLong();

        outKey.set(hour);
        outVal.set(cachedDist + ":" + requests);
        ctx.write(outKey, outVal);
    }
}
