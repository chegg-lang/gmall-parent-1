package com.atguigu.gmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;
    @GetMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String date){
        Long dauTotal = publisherService.getDauTotal(date); //根据日期获取从phoenix中查询到的数据
        Double orderAmount = publisherService.selectOrderAmount(date);
        List<Map> totalList = new ArrayList<>();//获取不同请求，每个请求是一个map集合

         Map dauMap = new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value" ,dauTotal);

        totalList.add(dauMap);

        Map newMidMap = new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",255);

        totalList.add(newMidMap);

        Map orderAmountMap = new HashMap();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        orderAmountMap.put("value",orderAmount);
        totalList.add(orderAmountMap);


        String jsonString = JSON.toJSONString(totalList);
        return jsonString;
    }

    @RequestMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id ,@RequestParam("date") String date){
        if(id.equals("dau")){
            Map dauHourMapTD = publisherService.getDauHour(date);
            String yd = getYd(date);
            Map dauHourMapYD = publisherService.getDauHour(yd);

            Map hourMap = new HashMap();
            hourMap.put("today",dauHourMapTD);
            hourMap.put("yesterday",dauHourMapYD);

            return JSON.toJSONString(hourMap);
        }else if(id.equals("order_amount")){
            Map orderAmountMapTD = publisherService.selectOrderHourAmount(date);
            String yd = getYd(date);
            Map orderAmountMapYD = publisherService.selectOrderHourAmount(yd);

            Map hourAmout = new HashMap();
            hourAmout.put("today",orderAmountMapTD);
            hourAmout.put("yesterday",orderAmountMapYD);

            return JSON.toJSONString(hourAmout);
        }else {
            return null;
        }
    }

    private String getYd(String td){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        try {
            Date today = simpleDateFormat.parse(td);
            Date yesterday = DateUtils.addDays(today, -1);
            return simpleDateFormat.format(yesterday);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

}
