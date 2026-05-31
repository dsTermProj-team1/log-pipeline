package com.logpipeline.mapreduce.baseline;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"window", "replicas"})
public class BaselineResult {
    @JsonProperty("window")
    private String window;

    @JsonProperty("replicas")
    private int replicas;

    public BaselineResult() {
    }

    public BaselineResult(String window, int replicas) {
        this.window = window;
        this.replicas = replicas;
    }

    public String getWindow() {
        return window;
    }

    public void setWindow(String window) {
        this.window = window;
    }

    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }
}
