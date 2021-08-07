package com.nanfeng.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.nanfeng.gmall.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author: nanfeng
  * @date: 2021/7/26 15:05
  * @description: 从Kafka中读取数据，根据表名进行分流处理(maxwell)
  */
object BaseDBMaxwellApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))

    var topic = "gmall0523_db_maxwell"
    var groupId = "base_db_maxwell_group"

    // 1、从Redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (null != offsetMap && offsetMap.size > 0) {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // 2、获取当前采集周期中，读取主题对应的分区以及偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    // 3、对读取的数据进行结构转换
    // ConsumerRecord[String, String] => V(jsonStr) => V(jsonObj)
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        jsonObj
      }
    }

    // 4、分流
    jsonObjDStream.foreachRDD {
      rdd => {
        rdd.foreach {
          jsonObj => {
            // 获取操作类型
            val opType: String = jsonObj.getString("type")
            val tableName: String = jsonObj.getString("table")
            val dataJsonObj: JSONObject = jsonObj.getJSONObject("data")

            if (dataJsonObj != null && !dataJsonObj.isEmpty) {
              if (("order_info" == tableName && "insert" == opType)
                || (tableName.equals("order_detail") && "insert" == opType)
                || tableName.equals("base_province")
                || tableName.equals("user_info")
                || tableName.equals("sku_info")
                || tableName.equals("base_trademark")
                || tableName.equals("base_category3")
                || tableName.equals("spu_info")) {

                var sendTopic = "ods_" + tableName

                MyKafkaSink.send(sendTopic, dataJsonObj.toString)
              }

            }
          }
        }
        // 手动提交偏移量
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }

    }
    ssc.start()
    ssc.awaitTermination()
  }
}