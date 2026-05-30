package com.logpipeline.mapreduce;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * 배치 집계 결과 = JSON Lines 한 줄의 출력 (DTO)
 * /results에 쓰이고 -> ES 인덱싱 -> Kibana 차트 필드로 쓰임
 * 출력 키 네이밍 (snake_case)은 이 클래스 한 곳에서만 관리
 */
@JsonPropertyOrder({"service", "hour", "requests", "count_4xx", "count_5xx", "error_rate"})
public class AggregationResult {
    private final String service;
    private final int hour;
    private final long requests;
    private final long count4xx;
    private final long count5xx;
    private final double errorRate;

    public AggregationResult(String service, int hour, long requests, long count4xx, long count5xx, double errorRate) {
        this.service = service;
        this.hour = hour;
        this.requests = requests;
        this.count4xx = count4xx;
        this.count5xx = count5xx;
        this.errorRate = errorRate;
    }

    @JsonProperty("hour")
    public int getHour() { return hour; }

    @JsonProperty("requests")
    public long getRequests() { return requests; }

    @JsonProperty("service")
    public String getService() { return service; }

    @JsonProperty("count_4xx")
    public long getCount4xx() { return count4xx; }

    @JsonProperty("count_5xx")
    public long getCount5xx() { return count5xx; }

    @JsonProperty("error_rate")
    public double getErrorRate() { return errorRate; }
}
