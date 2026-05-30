package com.logpipeline.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * 잡 진입점. hadoop jar mapreduce-job.jar <input> <output>로 실행
 * input 예: /logs/msa-logs/year=2024/month=01/day=15/hour=22
 * output 예: /results/hourly/2024-01-15-22
 */
public class LogAggregationDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        conf.setBoolean("mapreduce.job.user.classpath.first", true);

        String[] rest = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (rest.length < 2) {
            System.err.println("Usage: LogAggregationDriver <input> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "msa-log-hourly-aggregation");
        job.setJarByClass(LogAggregationDriver.class);

        job.setMapperClass(LogAggregationMapper.class);
        job.setCombinerClass(LogAggregationCombiner.class);
        job.setReducerClass(LogAggregationReducer.class);

        // Map 출력 타입 (Combiner와 동일)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AggWritable.class);

        // 최종 출력 타입
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(rest[0]));
        FileOutputFormat.setOutputPath(job, new Path(rest[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
