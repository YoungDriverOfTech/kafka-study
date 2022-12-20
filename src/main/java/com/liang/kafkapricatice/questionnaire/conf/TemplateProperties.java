package com.liang.kafkapricatice.questionnaire.conf;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "template")
@Data
public class TemplateProperties {

    private List<Template> templates;
    private int templateResultType; // 0-get file  1-get from db  2-es
    private String templateResultFilePath;


    @Data
    public static class Template {
        private String templateId;
        private String templateFilePath;
        private boolean active;
    }

}
