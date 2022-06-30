package com.jump.gmall0630.publisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @ClassName DauMapper
 * @Description
 * @Author Four5prings
 * @Date 2022/6/30 20:45
 * @Version 1.0
 */
public interface DauMapper {
    public Integer selectDauTotal(String date);

    /**
     * +-----+------+
     * | LH  |  CT  |
     * +-----+------+
     * | 20  | 908  |
     * +-----+------+
     * @param date
     * @return
     */
    public List<Map> selectDauTotalHourMap(String date);
}
