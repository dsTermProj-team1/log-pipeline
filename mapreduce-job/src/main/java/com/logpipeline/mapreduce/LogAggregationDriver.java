package com.logpipeline.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 잡 진입점. hadoop jar mapreduce-job.jar <input> <output>로 실행
 * input 예: /logs/msa-logs/year=2024/month=01/day=15/hour=22
 * output 예: /results/hourly/2024-01-15-22
 *
 * 출력은 두 갈래:
 *  - HDFS JSONL (기존 계약 그대로 — baseline 잡 / results-simulator 가 의존)
 *  - ES msa-batch 인덱스 (MultipleOutputs + EsOutputFormat, Reducer 에서 동시 적재)
 *    비활성화: -D agg.es.enabled=false (throughput 벤치 등 ES 불필요한 실행)
 */
public class LogAggregationDriver {

    /** 집계 윈도우 시작 시각(ISO-8601). ES 문서의 window_start / doc_id 에 쓰임 */
    public static final String WINDOW_KEY = "agg.window.start";
    /** false 면 ES 적재 생략(HDFS 출력만). 기본 true */
    public static final String ES_ENABLED_KEY = "agg.es.enabled";
    /** MultipleOutputs 네임드 출력 이름 */
    public static final String ES_NAMED_OUTPUT = "es";

    private static final Pattern PARTITION_PATTERN =
        Pattern.compile("year=(\\d{4})/month=(\\d{2})/day=(\\d{2})/hour=(\\d{2})");

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        conf.setBoolean("mapreduce.job.user.classpath.first", true);

        String[] rest = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (rest.length < 2) {
            System.err.println("Usage: LogAggregationDriver <input> <output>");
            System.exit(2);
        }

        // ── ES 적재 설정 (전부 -D 로 덮어쓰기 가능) ──
        if (conf.get("es.nodes") == null) {
            conf.set("es.nodes", "elasticsearch.monitoring.svc.cluster.local:9200");
        }
        if (conf.get("es.resource") == null) {
            conf.set("es.resource", "msa-batch");
        }
        conf.set("es.input.json", "true");          // Reducer 가 JSON 문자열(Text)을 그대로 넘김
        conf.setBoolean("es.nodes.wan.only", true); // k8s Service 주소만 사용(노드 디스커버리 생략)
        conf.set("es.mapping.id", "doc_id");        // 재실행 시 같은 문서 덮어씀 → 멱등
        // ES 쓰기는 롤백이 없으므로 투기적 실행 금지(중복 색인 방지)
        conf.setBoolean("mapreduce.map.speculative", false);
        conf.setBoolean("mapreduce.reduce.speculative", false);

        if (conf.get(WINDOW_KEY) == null) {
            conf.set(WINDOW_KEY, deriveWindowStart(rest[0]));
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

        // ES 네임드 출력. 등록만 하고 실제 연결은 Reducer 가 처음 write 할 때 일어나므로
        // agg.es.enabled=false 면 ES 가 내려가 있어도 잡은 영향 없음
        MultipleOutputs.addNamedOutput(job, ES_NAMED_OUTPUT,
            EsOutputFormat.class, NullWritable.class, Text.class);

        FileInputFormat.addInputPath(job, new Path(rest[0]));
        FileOutputFormat.setOutputPath(job, new Path(rest[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * 입력 파티션 경로(year=/month=/day=/hour=)에서 윈도우 시작 시각을 유도.
     * 경로에 파티션 패턴이 없으면(임시 디렉토리 등) 현재 정시(UTC)로 대체.
     */
    static String deriveWindowStart(String inputPath) {
        Matcher m = PARTITION_PATTERN.matcher(inputPath);
        if (m.find()) {
            return String.format("%s-%s-%sT%s:00:00Z", m.group(1), m.group(2), m.group(3), m.group(4));
        }
        return Instant.now().truncatedTo(ChronoUnit.HOURS).toString();
    }
}
