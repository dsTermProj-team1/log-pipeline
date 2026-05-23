package com.logpipeline.kafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;

@ConfigurationProperties(prefix = "app")
public class AppProperties {
    private final Kafka kafka;
    private final Hdfs hdfs;
    private final Rolling rolling;

    @ConstructorBinding
    public AppProperties(Kafka kafka, Hdfs hdfs, Rolling rolling) {
        this.kafka = kafka;
        this.hdfs = hdfs;
        this.rolling = rolling;
    }

    public Kafka getKafka() {
        return kafka;
    }

    public Hdfs getHdfs() {
        return hdfs;
    }

    public Rolling getRolling() {
        return rolling;
    }

    public static class Kafka {
        private final String topic;
        public Kafka(String topic) {this.topic = topic;}
        public String getTopic() {return topic;}
    }

    public static class Hdfs {
        private final String uri;
        private final String basePath;
        public Hdfs(String uri, String basePath) {this.uri = uri;this.basePath = basePath;}
        public String getUri() {return uri;}
        public String getBasePath() {return basePath;}
    }

    public static class Rolling {
        private final long maxBytes;
        private final long maxMessages;
        private final long checkIntervalMs;

        public Rolling(long maxBytes, long maxMessages, long checkIntervalMs) {
            this.maxBytes = maxBytes;
            this.maxMessages = maxMessages;
            this.checkIntervalMs = checkIntervalMs;
        }
        public long getMaxBytes() {return maxBytes;}
        public long getMaxMessages() {return maxMessages;}
        public long getCheckIntervalMs() {return checkIntervalMs;}
    }
}
