package com.jump.gmall0630.publisher.service.impl;

import com.jump.gmall0630.publisher.mapper.DauMapper;
import com.jump.gmall0630.publisher.mapper.OrderMapper;
import com.jump.gmall0630.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName PublisherServiceImpl
 * @Description
 * @Author Four5prings
 * @Date 2022/6/30 20:45
 * @Version 1.0
 */
@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private DauMapper dauMapper;
    @Autowired
    private OrderMapper orderMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map<String,Integer> getDauTotalHourMap(String date) {
        List<Map> list = dauMapper.selectDauTotalHourMap(date);
        HashMap<String, Integer> result = new HashMap<>();

        for (Map map : list) {
            result.put((String) map.get("LH"), (Integer) map.get("CT"));
        }
        return result;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String, Integer> getOrderAmountHourMap(String date) {
        ArrayList<Map> list = orderMapper.selectOrderAmountHourMap(date);
        HashMap<String, Integer> result = new HashMap<>();

        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Integer) map.get("SUM_AMOUNT"));
        }

        return result;
    }
}
