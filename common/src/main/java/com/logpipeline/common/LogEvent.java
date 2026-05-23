package com.logpipeline.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Kafka topic msa-logs 의 메시지 모델.
 * common/src/main/resources/schema/log-schema.json 과 1:1 대응.
 *
 * 필드 구분:
 *  - 탐지/분석 필수 필드: timestamp ~ hasValidToken (7개)
 *  - 리얼리티용 필드: userId ~ responseTimeMs (5개, nullable, 탐지 미사용)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LogEvent {

    // ── 탐지/분석 필수 필드 ──
    private String timestamp;
    private String service;
    private String ip;
    private String method;
    private String uri;
    private int status;

    @JsonProperty("has_valid_token")
    private boolean hasValidToken;

    // ── 리얼리티용 필드 (탐지 미사용, nullable) ──
    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("content_id")
    private String contentId;

    private String genre;

    @JsonProperty("watch_ratio")
    private Double watchRatio;          // null 가능 → 박싱 타입

    @JsonProperty("response_time_ms")
    private Integer responseTimeMs;     // null 가능 → 박싱 타입

    public LogEvent() {}

    // ── 필수 필드 getter/setter ──
    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public String getService() { return service; }
    public void setService(String service) { this.service = service; }

    public String getIp() { return ip; }
    public void setIp(String ip) { this.ip = ip; }

    public String getMethod() { return method; }
    public void setMethod(String method) { this.method = method; }

    public String getUri() { return uri; }
    public void setUri(String uri) { this.uri = uri; }

    public int getStatus() { return status; }
    public void setStatus(int status) { this.status = status; }

    public boolean isHasValidToken() { return hasValidToken; }
    public void setHasValidToken(boolean hasValidToken) { this.hasValidToken = hasValidToken; }

    // ── 리얼리티 필드 getter/setter ──
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getContentId() { return contentId; }
    public void setContentId(String contentId) { this.contentId = contentId; }

    public String getGenre() { return genre; }
    public void setGenre(String genre) { this.genre = genre; }

    public Double getWatchRatio() { return watchRatio; }
    public void setWatchRatio(Double watchRatio) { this.watchRatio = watchRatio; }

    public Integer getResponseTimeMs() { return responseTimeMs; }
    public void setResponseTimeMs(Integer responseTimeMs) { this.responseTimeMs = responseTimeMs; }
}
