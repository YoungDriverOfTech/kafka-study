package com.liang.kafkapricatice.questionnaire.service;

import com.alibaba.fastjson.JSONObject;
import com.liang.kafkapricatice.questionnaire.conf.TemplateProperties;
import com.liang.kafkapricatice.questionnaire.utils.FileUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
@Slf4j
public class QuestionnaireServiceImpl implements QuestionnaireService{

    @Autowired
    private TemplateProperties properties;

    @Override
    public TemplateProperties.Template getTemplate() {
        List<TemplateProperties.Template> templates = properties.getTemplates();
        Optional<TemplateProperties.Template> first = templates.stream().filter(TemplateProperties.Template::isActive).findFirst();
        return first.orElse(null);
    }

    @Override
    public void templateReported(JSONObject reportInfo) {
        // kafka producer 把数据推送到kafka
        log.info("templateReported: [{}]", reportInfo);
    }

    @Override
    public JSONObject templateStatistics(String templateId) {
        if (properties.getTemplateResultType() == 0) {
            // 文件获取
            return FileUtils.readFile2JsonObject(properties.getTemplateResultFilePath()).get();
        } else {
            // db ...
        }
        return null;
    }
}
