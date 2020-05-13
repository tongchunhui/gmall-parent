package com.nsn.gmall.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.nsn.common.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    // 接收 mock 发送的日志
    @PostMapping("/log")
    public String log(@RequestParam("logString") String logString){

        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());
        // 发送到kafka
        if( "startup".equals(jsonObject.get("type"))){
            kafkaTemplate.send(GmallConstant.KAFKA_STARTUP, jsonObject.toJSONString());
        } else {
            kafkaTemplate.send(GmallConstant.KAFKA_EVENT, jsonObject.toJSONString());
        }
        // 日志输出到文件，接着就处理这个日志文件
        log.info(logString);
        return "";
    }
}
