package com.atguigu.gmall.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

//标识为 controller 组件，交给 Sprint 容器管理，并接收处理请求 如果返回 String，会当作网页进行跳转

// @RestController = @Controller + @ResponseBody 会将返回结果转换为 json 进行响应
//@Controller
@RestController
@Slf4j
public class LoggerController {

    // Spring提供的对Kafka的支持
    @Autowired
    KafkaTemplate kafkaTemplate;

    //通过 requestMapping 匹配请求并交给方法处理
    @RequestMapping("/applog")
    //在模拟数据生成的代码中，我们将数据封装为 json，通过post传递给该Controller处理，所以我们通过@RequestBody 接收
    public String applog(@RequestBody String mockLog){
        // 落盘
        log.info(mockLog);

        // 根据日志的类型，发送到kafka的不同主题去
        // 将接受到的字符串数据转换为json对象
        JSONObject jsonObject = JSON.parseObject(mockLog);
        JSONObject startJson = jsonObject.getJSONObject("start");
        if (null != startJson){
            // 启动日志
            kafkaTemplate.send("gmall_start_0523", mockLog);
        }else {
            // 事件日志
            kafkaTemplate.send("gmall_event_0523", mockLog);
        }

        return "success";
    }
}
