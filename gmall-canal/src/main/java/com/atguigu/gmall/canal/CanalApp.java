package com.atguigu.gmall.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalApp {

    public static void main(String[] args) {

        //1.建立与mysql的连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while(true){
            canalConnector.connect();
            //2.订阅mysql中的数据
            canalConnector.subscribe("*.*");
            Message message = canalConnector.get(100);

            int size = message.getEntries().size();

            if (size==0){
                System.out.println("没有数据，休息5秒");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{

                for (CanalEntry.Entry entry : message.getEntries()) {

                    if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) { //只有数据处理相关的操作

                        ByteString storeValue = entry.getStoreValue();
                        CanalEntry.RowChange rowChange = null;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);  //反序列化
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }

                        //表执行了那些操作，数据变化后的数据集
                        //行集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        //表名
                        String tableName = entry.getHeader().getTableName();
                        //操作类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        CanalHandler canalHandler = new CanalHandler(eventType, tableName, rowDatasList);
                        canalHandler.handle();
                    }
                }

            }
        }
        //3.抓取message
    }
}
