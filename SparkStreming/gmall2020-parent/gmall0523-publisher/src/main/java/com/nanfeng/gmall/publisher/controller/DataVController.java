package com.nanfeng.gmall.publisher.controller;

import com.nanfeng.gmall.publisher.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class DataVController {

    @Autowired
    private MySQLService mySQLServiceImpl;

    @GetMapping("trademark-sum")
    public Object trademarkSum(@RequestParam("start_date") String startDate,
                               @RequestParam("end_date") String endDate,
                               @RequestParam("topN") int topN){
        List<Map> rsList = new ArrayList<>();

        List<Map> trademarkSum = mySQLServiceImpl.getTrademarkStat(startDate, endDate, topN);
        for (Map map : trademarkSum) {
            Map rsMap = new HashMap();
            rsMap.put("x", map.get("trademark_name"));
            rsMap.put("y", map.get("amount"));
            rsMap.put("s", 1);
            rsList.add(rsMap);
        }

        return rsList;
    }
}
