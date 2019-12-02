package com.atguigu.gmall.publisher.service;

import org.mybatis.spring.annotation.MapperScan;

import java.util.Map;

public interface PublisherService {
    public Long getDauTotal(String date);

    public Map getDauHour(String date);

    public Double selectOrderAmount(String date);

    public Map selectOrderHourAmount(String date);
}
