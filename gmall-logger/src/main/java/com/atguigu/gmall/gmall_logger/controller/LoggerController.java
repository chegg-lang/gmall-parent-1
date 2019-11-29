package com.atguigu.gmall.gmall_logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.common.constant.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController  //RestController会将返回的作为字符串，而Controller会将返回作为页面去查找页面
@Slf4j
public class LoggerController {

    @Autowired  //依赖注入
    KafkaTemplate<String,String>  kafkaTemplate;

    @PostMapping("log")
    public String log( @RequestParam("logString") String logString){
        //System.out.println(logString);

        //1.补时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());
        //2.落盘日志文件
        String jsonString = jsonObject.toJSONString();
        log.info(jsonString);

        //发送kafka
        if(jsonObject.getString("type").equals("startup")){

            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,jsonString);
        }else{
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonString);
        }

        return "Succerss";
    }
}
