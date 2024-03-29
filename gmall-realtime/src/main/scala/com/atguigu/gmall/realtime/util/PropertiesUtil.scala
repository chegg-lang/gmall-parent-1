package com.atguigu.gmall.realtime.util

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {

  //此实例对象的作用是读取config.properties配置文件
  def main(args: Array[String]): Unit = {
    val properties: Properties = PropertiesUtil.load("config.properties")

    println(properties.getProperty("kafka.broker.list"))
  }

  def load(propertieName:String): Properties ={
    val prop=new Properties();
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }

}

