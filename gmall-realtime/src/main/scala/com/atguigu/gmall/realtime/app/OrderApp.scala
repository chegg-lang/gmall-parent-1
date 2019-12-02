package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat


import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.bean.OrderInfo
import com.atguigu.gmall.common.constant.GmallConstant
import com.atguigu.gmall.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._;

object OrderApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")

    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)

    val orderDstream: DStream[OrderInfo] = inputDstream.map { record => { //数据格式位json字符串
      val jsonString: String = record.value()

      val orderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])

      //电话脱敏
      val tuple: (String, String) = orderInfo.consignee_tel.splitAt(3)

      orderInfo.consignee_tel = tuple._1 + "****" + tuple._2.splitAt(4)._2

      val dateAndTimeArray: Array[String] = orderInfo.create_time.split(" ")

      orderInfo.create_date = dateAndTimeArray(0)
      orderInfo.create_hour = dateAndTimeArray(1).split(":")(0)
      //订单上增加字段，该订单是否时该用户首次下单
      orderInfo

    }
    }
    orderDstream.foreachRDD{rdd=>{
      rdd.saveToPhoenix("GMALL2019_ORDER_INFO",
        Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT",
          "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID",
          "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS",
          "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID",
          "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        new Configuration(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    }}

    ssc.start()
    ssc.awaitTermination()

  }
}
