package com.atguigu.gmall.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {

    //求日活总数
    public Long getDauTotal(String date); //通过日期返回当日的日活用户

    //求日活分时总数

    public List<Map> getDauHour(String date);
}
