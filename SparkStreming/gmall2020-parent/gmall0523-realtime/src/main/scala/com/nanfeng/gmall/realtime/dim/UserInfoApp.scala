package com.nanfeng.gmall.realtime.dim

import java.text.SimpleDateFormat

import java.util
import com.alibaba.fastjson.JSON
import com.nanfeng.gmall.realtime.bean.UserInfo
import com.nanfeng.gmall.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author: nanfeng
  * @date: 2021/7/29 23:05
  * @description: 从Kafka中读取数据，保存到Phoenix
  */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))

    var topic = "ods_user_info"
    var groupId = "user_info_group"

    /** **************************************************************
      * 1、从Kafka中获取数据
      * **************************************************************/
    // 1.1获取本批次偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    // 1.2 从Kafka中读取数据
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (null != offsetMap && offsetMap.size > 0) {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }


    // 获取偏移量，并进行结构转换
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream = recordDStream.transform {
      consumerRecord => {
        // 1.3获取当前批次偏移量
        offsetRanges = consumerRecord.asInstanceOf[HasOffsetRanges].offsetRanges

        consumerRecord
      }
    }

    // 1.4进行结构转换 => UserInfo
    val userInfoDStream: DStream[UserInfo] = offsetDStream.transform {
      consumerRecord => {
        val userInfoRDD: RDD[UserInfo] = consumerRecord.map {
          consumerRecord => {
            // 获取UserInfo JsonStr
            val jsonStr: String = consumerRecord.value()
            val userInfo: UserInfo = JSON.parseObject(jsonStr, classOf[UserInfo])

            //把生日转换成年龄
            val formattor = new SimpleDateFormat("yyyy-MM-dd")
            val date: util.Date = formattor.parse(userInfo.birthday)
            val curTs: Long = System.currentTimeMillis()
            val betweenMs = curTs - date.getTime
            val age = betweenMs / 1000L / 60L / 60L / 24L / 365L
            if (age < 20) {
              userInfo.age_group = "20 岁及以下"
            } else if (age > 30) {
              userInfo.age_group = "30 岁以上"
            } else {
              userInfo.age_group = "21 岁到 30 岁"
            }
            if (userInfo.gender == "M") {
              userInfo.gender_name = "男"
            } else {
              userInfo.gender_name = "女"
            }

            userInfo
          }
        }

        userInfoRDD
      }
    }

    /** **************************************************************
      * 2、用Phoenix保存数据据
      * **************************************************************/
    // 2.1保存到Phoenix
    import org.apache.phoenix.spark._
    userInfoDStream.foreachRDD {
      userInfo => {
        userInfo.saveToPhoenix(
          "GMALL0523_USER_INFO",
          Seq("ID", "USER_LEVEL", "BIRTHDAY", "GENDER", "AGE_GROUP", "GENDER_NAME"),
          new Configuration,
          Some("master,salve1,slave2:2181")
        )

        // 2.2更新偏移量
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
