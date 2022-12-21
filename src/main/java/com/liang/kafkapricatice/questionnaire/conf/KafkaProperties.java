package com.liang.kafkapricatice.questionnaire.conf;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "questionnaire.kafka")
public class KafkaProperties {
    private String bootstrapServers;
    private String acksConfig;
}
