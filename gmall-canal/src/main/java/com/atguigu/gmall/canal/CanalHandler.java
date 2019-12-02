package com.atguigu.gmall.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall.canal.util.MyKafkaSender;
import com.atguigu.gmall.common.constant.GmallConstant;

import java.util.List;

public class CanalHandler {

    CanalEntry.EventType eventType;

    String tableName;

    List<CanalEntry.RowData> rowDataList;

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;
    }

    public void handle(){
        if(tableName.equals("order_info")&&eventType.equals(CanalEntry.EventType.INSERT)){  //下单信息
            rowDataToKafka(GmallConstant.KAFKA_TOPIC_ORDER);
        }
    }

    private void rowDataToKafka(String topic ){

        for (CanalEntry.RowData rowData : rowDataList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                System.out.println(column.getName() + "------->>" + column.getValue());
                jsonObject.put(column.getName(),column.getValue());
            }
            MyKafkaSender.send(topic,jsonObject.toJSONString());
        }
    }


}
