package com.nanfeng.gmall.publisher.service.impl;

import com.nanfeng.gmall.publisher.mapper.OrderWideMapper;
import com.nanfeng.gmall.publisher.service.ClickHouseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ClickHouseServiceImpl implements ClickHouseService {

    @Autowired
    private OrderWideMapper orderWideMapper;

    @Override
    public BigDecimal getOrderAmountTotal(String date) {
        return orderWideMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String, BigDecimal> getOrderAmountHour(String date) {
        Map<String, BigDecimal> rsMap = new HashMap<>();

        List<Map> mapList = orderWideMapper.selectOrderAmountHour(date);
        //List<Map{hr->11, am->10000}>  => Map{11->10000, 12->20000}
        for (Map map : mapList) {
            // 注意： key的名称不能随便写，得和mapper映射文件中，查询语句的别名一致
            rsMap.put(String.format("%02d", map.get("hr")), (BigDecimal) map.get("am"));
        }

        return rsMap;
    }
}
