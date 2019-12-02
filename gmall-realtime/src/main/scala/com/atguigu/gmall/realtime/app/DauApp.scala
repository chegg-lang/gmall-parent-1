package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.bean.StartUpLog
import com.atguigu.gmall.common.constant.GmallConstant
import com.atguigu.gmall.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._ //引入的是隐式转换的包
object DauApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")

    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)

//    inputDstream.foreachRDD(rdd=>
//      println(rdd.map(_.value()).collect.mkString("\n"))
//    )

    //业务逻辑
    //1.当日用户访问的清单保存到redis中
    //2.利用redis中的清单进行过滤


    val startlogDstream: DStream[StartUpLog] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val startUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])

      val date: Date = new Date(startUpLog.ts)
      val formattor = new SimpleDateFormat("yyyy-MM-dd HH")
      val dataString: String = formattor.format(date)
      val dateArr: Array[String] = dataString.split(" ")

      startUpLog.logDate = dateArr(0)
      startUpLog.logHour = dateArr(1)

      startUpLog
    }

    val filteredDstream: DStream[StartUpLog] = startlogDstream.transform { rdd =>
      //driver周期性的查询redis的清单 通过广播变量发送到excutor中

      println("过滤前：" + rdd.count())
      val jedis = new Jedis("hadoop102", 6379)
      val dateStr = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
      val dateKey = "dau:" + dateStr
      val dateMidSet: util.Set[String] = jedis.smembers(dateKey)
      jedis.close()

      val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dateMidSet)

      val filteredRDD: RDD[StartUpLog] = rdd.filter { startuplog => { //excutor端执行 根据
        //广播变量 比对自己的数据进行过滤
        val midSet: util.Set[String] = dauMidBC.value
        !midSet.contains(startuplog.mid)
      }
      }

      println("过滤后的数据" + filteredRDD.count())
      filteredRDD
    }


    val startupGroupByMidDstream: DStream[(String, Iterable[StartUpLog])] = filteredDstream.map(startuplog=>(startuplog.mid,startuplog)).groupByKey()

    val filtered2Dstream = startupGroupByMidDstream.flatMap {
      case (mid, startuplogItr) =>
        val topLogList: List[StartUpLog] = {
          startuplogItr.toList.sortWith { (startuplog1, startuplog2) =>
            startuplog1.ts < startuplog2.ts
          }
        }.take(1)
        topLogList
    }
    filtered2Dstream.cache()



    //保存 redis type:set            key: dau:2019-11-26         value:mid
    filtered2Dstream.foreachRDD { rdd =>

      //分区数据利用foreachPartition会将连接对象只再本分区中创建一次，
      //可以省去foreach对于每一个数据new一个连接对象的资源与时间

      rdd.foreachPartition(startuplogItr=>{
        val jedis = new Jedis("hadoop102",6379)

        //set的写入
        for (startuplog <- startuplogItr) {
          val dateKey:String = "dau:" + startuplog.logDate   //以日期字段作为主要的日活指标
          jedis.sadd(dateKey,startuplog.mid)
        }
        jedis.close()
      })
    }

    filtered2Dstream.foreachRDD{rdd=>      //foreachRDD的作用：
      //ctrl +shift +u 切换大小写
      rdd.saveToPhoenix("GMALL2019_DAU",Seq("MID", "UID", "APPID",
        "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        new Configuration ,Some("hadoop102,hadoop103,hadoop104:2181"))
  }
    ssc.start()
    ssc.awaitTermination()
  }
}
