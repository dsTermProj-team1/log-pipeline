package com.logpipeline.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Kafka topic msa-logs 의 메시지 모델.
 * log-schema.json 과 1:1 대응되어야 한다.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LogEvent {

    private String timestamp;
    private String service;
    private String method;
    private String uri;
    private int status;
    private String ip;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("has_valid_token")
    private boolean hasValidToken;

    @JsonProperty("latency_ms")
    private int latencyMs;

    @JsonProperty("user_agent")
    private String userAgent;

    public LogEvent() {}

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public String getService() { return service; }
    public void setService(String service) { this.service = service; }

    public String getMethod() { return method; }
    public void setMethod(String method) { this.method = method; }

    public String getUri() { return uri; }
    public void setUri(String uri) { this.uri = uri; }

    public int getStatus() { return status; }
    public void setStatus(int status) { this.status = status; }

    public String getIp() { return ip; }
    public void setIp(String ip) { this.ip = ip; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public boolean isHasValidToken() { return hasValidToken; }
    public void setHasValidToken(boolean hasValidToken) { this.hasValidToken = hasValidToken; }

    public int getLatencyMs() { return latencyMs; }
    public void setLatencyMs(int latencyMs) { this.latencyMs = latencyMs; }

    public String getUserAgent() { return userAgent; }
    public void setUserAgent(String userAgent) { this.userAgent = userAgent; }
}
