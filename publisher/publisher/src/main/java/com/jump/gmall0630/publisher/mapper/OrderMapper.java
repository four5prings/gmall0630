package com.jump.gmall0630.publisher.mapper;

import java.util.ArrayList;
import java.util.Map;

/**
 * @ClassName OrderMapper
 * @Description
 * @Author Four5prings
 * @Date 2022/6/30 22:18
 * @Version 1.0
 */
public interface OrderMapper {
    public Double selectOrderAmountTotal(String date);
    /*
    +--------------+-------------+
    | CREATE_HOUR  | SUM_AMOUNT  |
    +--------------+-------------+
    +--------------+-------------+
     */
    public ArrayList<Map> selectOrderAmountHourMap(String date);
}
