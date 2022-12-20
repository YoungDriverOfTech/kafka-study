package com.liang.kafkapricatice.questionnaire.service;

import com.alibaba.fastjson.JSONObject;
import com.liang.kafkapricatice.questionnaire.conf.TemplateProperties;

public interface QuestionnaireService {
    /*
    * get template
    * */
    TemplateProperties.Template getTemplate();

    /*
    * report questionnaire
    * */
    void templateReported(JSONObject reportInfo);

    /*
    * get result
    * */
    JSONObject templateStatistics(String templateId);
}
