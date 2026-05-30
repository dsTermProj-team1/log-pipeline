package com.logpipeline.mapreduce;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 같은 (service, hour) 키의 부분합들을 최종 합산 -> JSON Lines 한 줄로 출력
 * 출력 포맷/키 네이밍은 Reducer가 모름 전부 AggregationResult가 책임
 */
public class LogAggregationReducer extends Reducer<Text, AggWritable, NullWritable, Text> {
    private final ObjectMapper mapper = new ObjectMapper();
    private final Text outVal = new Text();

    @Override
    protected void reduce(Text key, Iterable<AggWritable> values, Context context) throws IOException, InterruptedException {
        long requests = 0, c4xx = 0, c5xx = 0;
        for (AggWritable v : values) {
            requests += v.getRequests();
            c4xx += v.getCount4xx();
            c5xx += v.getCount5xx();
        }

        String[] parts = key.toString().split("\t");
        String service = parts[0];
        int hour = Integer.parseInt(parts[1]);

        long errors = c4xx + c5xx;
        double errorRate = requests == 0 ? 0.0 :
            Math.round((double) errors / requests * 1e4) / 1e4;
        AggregationResult result =
            new AggregationResult(service, hour, requests, c4xx, c5xx, errorRate);

        outVal.set(mapper.writeValueAsString(result));
        context.write(NullWritable.get(), outVal);
    }
}
