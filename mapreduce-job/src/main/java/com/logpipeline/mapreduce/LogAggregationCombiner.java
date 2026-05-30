package com.logpipeline.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Map 단계 직후, 노드 로컬에서 부분 합산
 * 셔플로 네트워크 타기 전에 같은 키끼리 미리 줄여서 트래픽 절감
 * 입력/출력 타입이 Map 출력 (Text, AggWritable)과 동일해야함 - Reducer 와 별도 클래스
 */
public class LogAggregationCombiner extends Reducer <Text, AggWritable, Text, AggWritable>{
    private final AggWritable outVal = new  AggWritable();

    @Override
    protected void reduce(Text key, Iterable<AggWritable> values, Context context) throws IOException, InterruptedException {
        long requests = 0, c4xx = 0, c5xx = 0;
        for(AggWritable v : values){
            requests += v.getRequests();
            c4xx += v.getCount4xx();
            c5xx += v.getCount5xx();
        }
        outVal.set(requests, c4xx, c5xx);
        context.write(key, outVal);
    }
}
