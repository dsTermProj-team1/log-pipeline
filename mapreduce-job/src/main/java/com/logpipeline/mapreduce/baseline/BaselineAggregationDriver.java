package com.logpipeline.mapreduce.baseline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class BaselineAggregationDriver extends Configured implements Tool {

    private static final String[] WINDOW_LABELS = {"00-06", "06-12", "12-18", "18-24"};
    private static final int[][] WINDOW_HOURS = {
            {0, 1, 2, 3, 4, 5}, {6, 7, 8, 9, 10, 11},
            {12, 13, 14, 15, 16, 17}, {18, 19, 20, 21, 22, 23}
    };
    private static final double CEIL_EPS = 1e-9;

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: BaselineAggregationDriver <input> <ewmaTmp> <finalOut>");
            return 2;
        }
        Configuration conf = getConf();
        if (conf.get("baseline.ref.date") == null) {
            conf.set("baseline.ref.date", LocalDate.now().toString());
        }
        Path input = new Path(args[0]);
        Path ewmaTmp = new Path(args[1]);
        Path finalOut = new Path(args[2]);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(ewmaTmp, true);

        // 분산 EWMA 집계
        Job job = Job.getInstance(conf, "baseline-hour-ewma");
        job.setJarByClass(BaselineAggregationDriver.class);
        job.setMapperClass(HourEwmaMapper.class);
        job.setReducerClass(HourEwmaReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 64L * 1024 * 1024);
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, ewmaTmp);

        if (!job.waitForCompletion(true)) {
            return 1;
        }

        // 24개 EWMA -> 구간 파드 수
        Map<Integer, Double> ewma = readEwma(fs, ewmaTmp);
        if (ewma.isEmpty()) {
            System.err.println("[baseline] EWMA 결과 비어 있음 - 입력 데이터 확인");
            return 1;
        }
        List<BaselineResult> results = computeWindows(conf, ewma);

        // 출력
        writeBaseLine(fs, conf, finalOut, results);
        for (BaselineResult r : results) {
            System.out.printf("[baseline] %s -> %d pod%n", r.getWindow(), r.getReplicas());
        }
        return 0;
    }

    private Map<Integer, Double> readEwma(FileSystem fs, Path dir) throws Exception {
        Map<Integer, Double> map = new TreeMap<>();
        for (FileStatus st : fs.listStatus(dir)) {
            if (st.getPath().getName().startsWith("_")) {
                continue;
            }
            try (FSDataInputStream in = fs.open(st.getPath());

                 BufferedReader br = new BufferedReader(
                         new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) {
                        continue;
                    }
                    String[] parts = line.split("\\s+");
                    map.put(Integer.parseInt(parts[0]), Double.parseDouble(parts[1]));

                }
            }
        }
        return map;
    }

    private List<BaselineResult> computeWindows(Configuration conf, Map<Integer, Double> ewma) {
        int anchor = conf.getInt("baseline.anchor.pods", 4);
        int minPods = conf.getInt("baseline.min.pods", 1);
        int maxPods = conf.getInt("baseline.max.pods", 6);

        double peak = 0.0;
        for (double v : ewma.values()) {
            peak = Math.max(peak, v);
        }
        double capacity = peak / anchor;

        List<BaselineResult> out = new ArrayList<>();
        for (int w = 0; w < WINDOW_LABELS.length; w++) {
            double wmax = 0.0;
            for (int h : WINDOW_HOURS[w]) {
                Double v = ewma.get(h);
                if (v != null) {
                    wmax = Math.max(wmax, v);
                }
            }
            int pods = (int) Math.ceil(wmax / capacity - CEIL_EPS);
            pods = Math.max(minPods, Math.min(pods, maxPods));
            out.add(new BaselineResult(WINDOW_LABELS[w], pods));
        }
        return out;
    }

    private void writeBaseLine(FileSystem fs, Configuration conf, Path finalOut, List<BaselineResult> results) throws Exception {
        fs.delete(finalOut, true);
        fs.mkdirs(finalOut);
        ObjectMapper json = new ObjectMapper();
        Path part = new Path(finalOut, "part-r-00000");
        try (FSDataOutputStream os = fs.create(part, true)) {
            for (BaselineResult r : results) {
                os.write((json.writeValueAsString(r) + "\n").getBytes(StandardCharsets.UTF_8));
            }
        }
        fs.create(new Path(finalOut, "_SUCCESS"), true).close();
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new BaselineAggregationDriver(), args));
    }

}
