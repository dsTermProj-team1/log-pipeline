package com.logpipeline.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.logpipeline.kafka.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Kafka → HDFS 싱크 컨슈머
 */
@SpringBootApplication
@EnableConfigurationProperties(AppProperties.class)
@EnableScheduling
public class HdfsSinkConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(HdfsSinkConsumerApplication.class, args);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
