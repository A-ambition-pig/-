package com.nanfeng.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.nanfeng.gmall.realtime.bean.ProvinceInfo
import com.nanfeng.gmall.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author: nanfeng
  * @date: 2021/7/29 22:34
  * @description: 从Kafka中读取省份信息，保存到Phoenix
  */
object ProvinceInfoApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))

    var topic = "ods_base_province"
    var groupId = "province_info_group"

    /****************************************************************
      * 1、从Kafka中获取数据
      ***************************************************************/
    // 获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    // 获取数据
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (null != offsetMap && offsetMap.size > 0) {
       recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    }else {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // 获取当前批次获取偏移量情况
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    /****************************************************************
      * 2、保存数据到Phoenix
      ***************************************************************/
    import org.apache.phoenix.spark._
    offsetDStream.foreachRDD{
      rdd => {
        // 结构转换
        val provinceInfoRDD: RDD[ProvinceInfo] = rdd.map {
          record => {
            // 获取省份Json格式字符串
            val jsonStr: String = record.value()
            val provinceInfo: ProvinceInfo = JSON.parseObject(jsonStr, classOf[ProvinceInfo])
            provinceInfo
          }
        }

        // 保存到HBase
        provinceInfoRDD.saveToPhoenix(
          "GMALL0523_PROVINCE_INFO",
          Seq("ID","NAME","AREA_CODE","ISO_CODE"),
          new Configuration,
          Some("master,slave1,slave2:2181")
        )

        //保存偏移量
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
