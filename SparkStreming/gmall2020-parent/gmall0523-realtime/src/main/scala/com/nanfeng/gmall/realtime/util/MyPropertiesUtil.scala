package com.nanfeng.gmall.realtime.util

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
  * @author: nanfeng
  * @date: 2021/7/20 20:27
  * @description:
  */
object MyPropertiesUtil {

  def main(args: Array[String]): Unit = {
    val properties: Properties = MyPropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }

  def load(propertiesName: String): Properties = {
    val props = new Properties()
    // 加载指定的配置文件
    props.load(new InputStreamReader(
      Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
      StandardCharsets.UTF_8))

    props
  }
}
