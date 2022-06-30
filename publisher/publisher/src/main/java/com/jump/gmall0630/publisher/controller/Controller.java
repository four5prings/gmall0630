package com.jump.gmall0630.publisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.jump.gmall0630.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName Controller
 * @Description
 * @Author Four5prings
 * @Date 2022/6/30 20:45
 * @Version 1.0
 */
@RestController
public class Controller {
    @Autowired
    private PublisherService publisherService;

    /*
     * 总数	http://localhost:8070/realtime-total?date=2021-08-15
     */
    @RequestMapping("realtime-total")
    public String realtimeTotal(@RequestParam String date) {
        Integer dauTotal = publisherService.getDauTotal(date);
        Double GmvTotal = publisherService.getOrderAmountTotal(date);
//创建list集合存放结果
        ArrayList<Map> result = new ArrayList<>();

        //创建新增日活集合
        //[{"id":"dau","name":"新增日活","value":1200},
        //{"id":"new_mid","name":"新增设备","value":233}
        // {"id":"order_amount","name":"新增交易额","value":1000.2 }]
        HashMap<String, Object> DauMap = new HashMap<>();
        HashMap<String, Object> MidMap = new HashMap<>();
        HashMap<String, Object> GmvMap = new HashMap<>();

        DauMap.put("id", "dau");
        DauMap.put("name", "新增日活");
        DauMap.put("value", dauTotal);

        MidMap.put("id", "new_mid");
        MidMap.put("name", "新增设备");
        MidMap.put("value", 233);

        GmvMap.put("id", "order_amount");
        GmvMap.put("name", "新增交易额");
        GmvMap.put("value", GmvTotal);

        result.add(DauMap);
        result.add(MidMap);
        result.add(GmvMap);

        return JSONObject.toJSONString(result);
    }

    /*
     * 分时统计	http://localhost:8070/realtime-hours?id=dau&date=2021-08-15
     * 分时统计  http://localhost:8070/realtime-hours?id=order_amount&date=2021-08-18
     */
    @RequestMapping("realtime-hours")
    public String realtimeHours(
            @RequestParam int id,
            @RequestParam String date) {
        HashMap<String, Object> result = new HashMap<>();
        //获取date的前一天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        Map<String, Integer> todayMap = null;
        Map<String, Integer> yesterdayMap = null;

        if ("dau".equals(id)) {
            //获取今天的日活数据
            todayMap = publisherService.getDauTotalHourMap(date);
            //获取前一天的日活数据
            yesterdayMap = publisherService.getDauTotalHourMap(yesterday);

        } else if ("order_amount".equals(id)) {
            todayMap = publisherService.getOrderAmountHourMap(date);
            yesterdayMap = publisherService.getOrderAmountHourMap(yesterday);
        }

        /*
        {"yesterday":{"11":383,"12":123,"17":88,"19":200 },
        "today":{"12":38,"13":1233,"17":123,"19":688 }}
         */
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        return JSONObject.toJSONString(result);
    }
}
