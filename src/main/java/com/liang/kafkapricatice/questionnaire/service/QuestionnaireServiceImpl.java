package com.liang.kafkapricatice.questionnaire.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.liang.kafkapricatice.questionnaire.conf.TemplateProperties;
import com.liang.kafkapricatice.questionnaire.utils.FileUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
@Slf4j
public class QuestionnaireServiceImpl implements QuestionnaireService{

    @Autowired
    private TemplateProperties properties;

    @Autowired
    private Producer producer;

    @Override
    public TemplateProperties.Template getTemplate() {
        List<TemplateProperties.Template> templates = properties.getTemplates();
        Optional<TemplateProperties.Template> first = templates.stream().filter(TemplateProperties.Template::isActive).findFirst();
        return first.orElse(null);
    }

    @Override
    public void templateReported(JSONObject reportInfo) {
        log.info("templateReported: [{}]", reportInfo);

        // compile record
        String templateId = reportInfo.getString("templateId");
        JSONArray reportData = reportInfo.getJSONArray("result");
        ProducerRecord<String, Object> record = new ProducerRecord<>("first-topic", templateId, reportData);

        /*
        * 1. kafka Producer是线程安全的，建议多线程复用。如果每个线程都创建，会出现大量的上下文切换，影响kafka效率
        * 2. kafka Producer的key是一个重要内容
        *    2.1 可以根据key完成partition的负载均衡
        *    2.2 合理的key设计可以让Flink, Spark streaming 的实时分析工具做更快速处理
        * 3. Ack=all，kafka层面上就已经有了只有一次的消息投递保障，但是如果想要不丢数据，最好自行处理异常
        *
        * */
        // send to kafka
        try {
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
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
