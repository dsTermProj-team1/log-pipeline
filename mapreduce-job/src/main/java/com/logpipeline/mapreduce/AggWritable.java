package com.logpipeline.mapreduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 집계 단위 값: (요청 수, 4xx 수, 5xx 수)
 * Mapper 출력값 - Combiner/Reducer 입력값으로 쓰임
 * Hadoop 이 네트워크로 셔플할 때 직렬화하므로 Writable 구현 필수
 */
public class AggWritable implements Writable {

    private long requests;
    private long count4xx;
    private long count5xx;

    public AggWritable() {}

    public void set(long requests, long count4xx, long count5xx) {
        this.requests = requests;
        this.count4xx = count4xx;
        this.count5xx = count5xx;
    }

    public long getRequests() {
        return requests;
    }

    public long getCount4xx() {
        return count4xx;
    }

    public long getCount5xx() {
        return count5xx;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(requests);
        dataOutput.writeLong(count4xx);
        dataOutput.writeLong(count5xx);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        requests = dataInput.readLong();
        count4xx = dataInput.readLong();
        count5xx = dataInput.readLong();
    }
}
