package com.nanfeng.gmall.publisher.service.impl;

import com.nanfeng.gmall.publisher.mapper.TrademarkStatMapper;
import com.nanfeng.gmall.publisher.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class MySQLServiceImpl implements MySQLService {

    @Autowired
    private TrademarkStatMapper trademarkStatMapper;

    @Override
    public List<Map> getTrademarkStat(String startDate, String endDate, int topN) {
        return trademarkStatMapper.selectTradeSum(startDate, endDate, topN);
    }
}
