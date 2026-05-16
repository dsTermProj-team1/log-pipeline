package com.logpipeline.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Kafka topic msa-logs 의 메시지 모델.
 * common/src/main/resources/schema/log-schema.json 과 1:1 대응.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LogEvent {

    private String timestamp;
    private String service;
    private String ip;
    private String method;
    private String uri;
    private int status;

    @JsonProperty("has_valid_token")
    private boolean hasValidToken;

    public LogEvent() {}

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
}
