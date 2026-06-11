package com.logpipeline.mapreduce;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * 같은 (service, hour) 키의 부분합들을 최종 합산 -> JSON Lines 한 줄로 출력
 * 출력 포맷/키 네이밍은 Reducer가 모름 전부 AggregationResult가 책임
 *
 * 출력 두 갈래:
 *  - context.write → HDFS JSONL. 기존 계약과 바이트 단위로 동일 (baseline 잡이 파싱)
 *  - mos.write("es") → ES msa-batch. HDFS 문서에 window_start(시계열 필드)와
 *    doc_id(service@window, 재실행 멱등용)를 더한 확장 문서
 */
public class LogAggregationReducer extends Reducer<Text, AggWritable, NullWritable, Text> {
    private final ObjectMapper mapper = new ObjectMapper();
    private final Text outVal = new Text();
    private final Text esVal = new Text();

    private MultipleOutputs<NullWritable, Text> mos;
    private boolean esEnabled;
    private String windowStart;

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        esEnabled = conf.getBoolean(LogAggregationDriver.ES_ENABLED_KEY, true);
        windowStart = conf.get(LogAggregationDriver.WINDOW_KEY, "");
        if (esEnabled) {
            mos = new MultipleOutputs<>(context);
        }
    }

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

        if (esEnabled) {
            ObjectNode doc = mapper.valueToTree(result);
            doc.put("window_start", windowStart);
            doc.put("doc_id", service + "@" + windowStart);
            esVal.set(doc.toString());
            mos.write(LogAggregationDriver.ES_NAMED_OUTPUT, NullWritable.get(), esVal);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (mos != null) {
            mos.close();   // ES bulk 버퍼 flush
        }
    }
}
