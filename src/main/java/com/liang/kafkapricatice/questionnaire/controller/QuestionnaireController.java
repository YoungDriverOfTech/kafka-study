package com.liang.kafkapricatice.questionnaire.controller;

import com.liang.kafkapricatice.questionnaire.common.BaseResponseVO;
import com.liang.kafkapricatice.questionnaire.conf.TemplateProperties;
import com.liang.kafkapricatice.questionnaire.service.QuestionnaireService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/v1")
public class QuestionnaireController {

    @Autowired
    private TemplateProperties properties;

    @Autowired
    private QuestionnaireService service;

    @RequestMapping(value = "/template", method = RequestMethod.GET)
    public BaseResponseVO getTemplate() {
        return BaseResponseVO.success();
    }

    @RequestMapping(value = "/template/result", method = RequestMethod.GET)
    public BaseResponseVO templateStatistics(@RequestParam (value = "templateId", required = false) String templateId) {
        return BaseResponseVO.success();
    }

    @RequestMapping(value = "/template/report", method = RequestMethod.POST)
    public BaseResponseVO dataReported(@RequestBody String reportData) {
        return BaseResponseVO.success();
    }


}
