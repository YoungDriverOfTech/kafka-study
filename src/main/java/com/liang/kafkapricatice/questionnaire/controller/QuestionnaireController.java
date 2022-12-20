package com.liang.kafkapricatice.questionnaire.controller;

import com.alibaba.fastjson.JSONObject;
import com.liang.kafkapricatice.questionnaire.common.BaseResponseVO;
import com.liang.kafkapricatice.questionnaire.conf.TemplateProperties;
import com.liang.kafkapricatice.questionnaire.service.QuestionnaireService;
import com.liang.kafkapricatice.questionnaire.utils.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/v1")
public class QuestionnaireController {

    @Autowired
    private TemplateProperties properties;

    @Autowired
    private QuestionnaireService service;

    @RequestMapping(value = "/template", method = RequestMethod.GET)
    public BaseResponseVO<Map<String, Object>> getTemplate() {

        TemplateProperties.Template template = service.getTemplate();

        Map<String, Object> result = new HashMap<>();
        result.put("templateId", template.getTemplateId());
        result.put("template", FileUtils.readFile2JsonArray(template.getTemplateFilePath()));

        return BaseResponseVO.success(result);
    }

    @RequestMapping(value = "/template/result", method = RequestMethod.GET)
    public BaseResponseVO<JSONObject> templateStatistics(@RequestParam (value = "templateId", required = false) String templateId) {

        JSONObject jsonObject = service.templateStatistics(templateId);

        return BaseResponseVO.success(jsonObject);
    }

    @RequestMapping(value = "/template/report", method = RequestMethod.POST)
    public BaseResponseVO dataReported(@RequestBody String reportData) {

        service.templateReported(JSONObject.parseObject(reportData));

        return BaseResponseVO.success();
    }


}
