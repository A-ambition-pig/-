package com.nanfeng.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.nanfeng.gmall.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author: nanfeng
  * @date: 2021/7/26 11:22
  * @description:
  */
object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BaseDBCanalApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))

    var topic = "gmall0523_db_canal"
    var groupId = "base_db_canal_group"

    // 1\从Redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (null != offsetMap && offsetMap.size > 0) {
      // 从指定的偏移量位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)

    }else {
      // 从最新的位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // 2、获取当前批次读取的Kafka主题中偏移量信息
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    // 3、对接收到的数据进行结构转换
    // ConsumerRecord[String, String(jsonStr)] ===> JsonObj
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        // json格式字符串
        val jsonStr: String = record.value()
        // jsonStr => jsonObj
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        jsonObj
      }
    }

    // 4、分流: 根据不同的表名发送到不同kafka主题中去
    // 获取首单，只关心type: insert状态
    jsonObjDStream.foreachRDD{
      rdd => {
        rdd.foreach{
          jsonObj => {
            // 获取操作类型
            val opType: String = jsonObj.getString("type")
            if ("INSERT".equals(opType) || "UPDATE".equals(opType)){
              // 获取表名
              val tableName: String = jsonObj.getString("table")
              // 获取数据 json数组
              val dataArr: JSONArray = jsonObj.getJSONArray("data")

              // 拼接目标topic名称
              var sendTopic = "ods_" + tableName
              // 对dataArr进行遍历
              import scala.collection.JavaConverters._
              for (data <- dataArr.asScala) {
                // 根据表名将数据发送到不同的主题中去
                MyKafkaSink.send(sendTopic, data.toString)
              }
            }

          }
        }
        // 提交偏移量
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
