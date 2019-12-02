package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.mapper.DauMapper;
import com.atguigu.gmall.publisher.mapper.OrderMapper;
import com.atguigu.gmall.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;
    @Override
    public Long getDauTotal(String date) {
        Long dauTotal = dauMapper.getDauTotal(date);
        return dauTotal;
    }

    @Override
    public Map getDauHour(String date) {
        List<Map> dauHourListMap = dauMapper.getDauHour(date);
        Map dauHourMap = new HashMap();

        for (Map map : dauHourListMap) {
            dauHourMap.put(map.get("loghour"),map.get("ct"));
            
        }
        return dauHourMap;
    }

    @Override
    public Double selectOrderAmount(String date) {
        Double orderAmount = orderMapper.selectOrderAmount(date);
        return orderAmount;
    }

    @Override
    public Map selectOrderHourAmount(String date) {
        List<Map> orderHourList = orderMapper.selectOrderAmountHour(date);
        HashMap orderHourMap = new HashMap();
        for (Map map : orderHourList) {
            orderHourMap.put(map.get("create_hour"),map.get("order_amount"));
        }

        return orderHourMap;
    }
}
