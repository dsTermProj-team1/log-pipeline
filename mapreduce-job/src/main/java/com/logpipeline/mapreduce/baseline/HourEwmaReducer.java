package com.logpipeline.mapreduce.baseline;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HourEwmaReducer extends Reducer<IntWritable, Text, IntWritable, DoubleWritable> {

    private double decay;
    private final DoubleWritable outVal = new DoubleWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        decay = context.getConfiguration().getDouble("baseline.decay", 0.7);
    }

    @Override
    protected void reduce(IntWritable hour, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // 날짜별 서비스 합산
        Map<Long, Long> byDist = new HashMap<>();
        for (Text v : values) {
            String s = v.toString();
            int sep = s.indexOf(':');
            long dist = Long.parseLong(s.substring(0, sep));
            long req = Long.parseLong(s.substring(sep + 1));
            byDist.merge(dist, req, Long::sum);
        }

        // EWMA (날짜 거리 가중)
        double num = 0.0;
        double den = 0.0;
        for (Map.Entry<Long, Long> e : byDist.entrySet()) {
            double w = Math.pow(decay, e.getKey());
            num += w * e.getValue();
            den += w;
        }
        double ewma = (den == 0.0) ? 0.0 : (num/den);

        outVal.set(ewma);
        context.write(hour, outVal);
    }
}
