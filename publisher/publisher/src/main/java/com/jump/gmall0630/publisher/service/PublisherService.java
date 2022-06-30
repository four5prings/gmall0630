package com.jump.gmall0630.publisher.service;

import java.util.Map;

public interface PublisherService {
    public Integer getDauTotal(String date);

    public Map<String,Integer> getDauTotalHourMap(String date);

    public Double getOrderAmountTotal(String date);

    public Map<String,Integer> getOrderAmountHourMap(String date);
}
