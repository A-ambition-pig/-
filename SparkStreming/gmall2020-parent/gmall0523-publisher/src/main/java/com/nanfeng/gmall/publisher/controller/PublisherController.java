package com.nanfeng.gmall.publisher.controller;

import com.nanfeng.gmall.publisher.service.ClickHouseService;
import com.nanfeng.gmall.publisher.service.ESService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 发布数据接口
 */
@RestController
public class PublisherController {

    @Autowired
    ESService esServiceImpl;

    @Autowired
    private ClickHouseService clickHouseServiceImpl;

    /**
     * 访问路径：
     * 相应数据：
     */
    @RequestMapping("/realtime-total")
    public Object realtimeTotal(@RequestParam("date") String dt) {
        // 返回数据集合
        List<Map<String, Object>> rsList = new ArrayList<>();
        Map<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        Long dauTotal = esServiceImpl.getDauTotal(dt);
        if (null == dauTotal) {
            dauMap.put("value", 0);
        }else {
            dauMap.put("value", dauTotal);
        }
        rsList.add(dauMap);

        Map<String, Object> midMap = new HashMap<>();
        midMap.put("id", "new_mid");
        midMap.put("name", "新增设备");
        midMap.put("value", 666);
        rsList.add(midMap);

        Map<String, Object> orderAmountMap = new HashMap<>();
        orderAmountMap.put("id", "order_amount");
        orderAmountMap.put("name", "新增交易额");
        orderAmountMap.put("value", clickHouseServiceImpl.getOrderAmountTotal(dt));
        rsList.add(orderAmountMap);

        return rsList;
    }

    /**
     *
     */
    @RequestMapping("/realtime-hour")
    public Object realtimeHour(@RequestParam("id") String id, @RequestParam("date") String date) {


        if ("dau".equals(id)){
            Map<String, Map<String, Long>> rsMap = new HashMap<>();
            // 获取今天的日活统计
            Map<String, Long> tdMap = esServiceImpl.getDauHour(date);
            rsMap.put("today", tdMap);

            //获取昨天的日活统计
            //根据当前日期获取昨天日期字符串
            String yd = getYd(date);
            Map<String, Long> ydMap = esServiceImpl.getDauHour(yd);
            rsMap.put("yesterday", ydMap);

            return rsMap;
        }else if ("order_amount".equals(id)){
            Map<String, Map<String, BigDecimal>> rsMap = new HashMap<>();

            // 获取今天的交易额
            Map<String, BigDecimal> todayMap = clickHouseServiceImpl.getOrderAmountHour(date);
            rsMap.put("today", todayMap);

            //获取昨天的交易额
            //根据当前日期获取昨天日期字符串
            String yd = getYd(date);
            Map<String, BigDecimal> ydMap = clickHouseServiceImpl.getOrderAmountHour(yd);
            rsMap.put("yesterday", ydMap);

            return rsMap;
        }else {
            return null;
        }
    }

    private String getYd(String date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yd = null;
        try {
            Date tdDate = dateFormat.parse(date);
            Date ydDate = DateUtils.addDays(tdDate, -1);
            yd = dateFormat.format(ydDate);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("日期格式转变失败");
        }
        return yd;
    }

}
