package com.nanfeng.gmall.publisher.mapper;



import org.apache.ibatis.annotations.Param;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * 订单宽表进行操作的接口
 */
public interface OrderWideMapper {

    // 获取指定日期的交易额
    BigDecimal selectOrderAmountTotal(@Param("date") String date);

    // 获取指定日期的分时交易额
    List<Map> selectOrderAmountHour(@Param("date") String date);
}
