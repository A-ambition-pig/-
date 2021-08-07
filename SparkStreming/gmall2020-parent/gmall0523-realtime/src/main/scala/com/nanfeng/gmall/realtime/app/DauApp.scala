package com.nanfeng.gmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.nanfeng.gmall.realtime.bean.DauInfo
import com.nanfeng.gmall.realtime.util.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * @author: nanfeng
  * @date: 2021/7/21 20:28
  * @description:
  */
object DauApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc = new StreamingContext(conf, Seconds(5))

    var topic = "gmall_start_0523"
    var groupId = "gmall_dau_0523"


    // 从Redis中获取Kafka分区偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (null != offsetMap && offsetMap.size > 0) {
      // 如果Redis中存在当前消费者组对该主题的偏移量信息
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    }else {
      // 没有，按照配置，从最新位置开始消费
      // 通过SparkStreaming从Kafka中读取数据
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // 获取当前采集周期，从Kafka中消费的数据起始偏移量以及结束偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        // 因为当前recordDStream底层封装的是KafkaRDD，混入HasOffsetRanges特质，提供了获取偏移量范围的方法
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        rdd
      }
    }


    // 从Kafka中读取json
    val jsonDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        //将json格式字符串转换为json对象
        val jSONObject: JSONObject = JSON.parseObject(jsonStr)
        // 从json中对象中获取时间戳
        val ts: lang.Long = jSONObject.getLong("ts")
        // 将时间戳转换为日期和小时 2021-07-21 20
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        val dateStrArr: Array[String] = dateStr.split(" ")
        val dt: String = dateStrArr(0)
        val hr: String = dateStrArr(1)

        jSONObject.put("dt", dt)
        jSONObject.put("hr", hr)
        jSONObject
      }
    }
    //jsonDStream.print(1000)

    // 对采集到的启动日志进行去重操作
    // 通过Redis  类型set Key: dau: 2021-07-21 value: mid expire 3600*24
    // 方案一 弊端：获取Jedis客户端连接频繁
    /*val filterDStream: DStream[JSONObject] = jsonDStream.filter {
      jsonObj => {
        // 获取登录日期
        val dt: String = jsonObj.getString("dt")
        //获取设备id
        val mid: String = jsonObj.getJSONObject("common").getString("mid")

        //拼接Redis中的key
        var dauKey = "dau: " + dt

        // 获取Jedis客户端
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        // 从Redis中判断当前设备是否已经登陆过
        val isFirst: lang.Long = jedis.sadd(dauKey, mid)
        // 设置key失效时间
        if (jedis.ttl(dauKey) < 0) {
          jedis.expire(dauKey, 3600 * 24)
        }
        // 关闭连接
        jedis.close()

        if (isFirst == 1L) {
          // 说明是第一次登录
          true
        } else {
          false
        }

      }
    }
    filterDStream.count().print()*/

    // 方案二 以分区为单位，对数据进行处理，每一个分区获取一次redis连接
    val filterDStream: DStream[JSONObject] = jsonDStream.mapPartitions {
      jsonObjIter => {
        // 以分区为单位进行处理
        //获取redis连接
        val jedis: Jedis = MyRedisUtil.getJedisClient()

        //定义一个集合，用于存放当前分区中第一次登录的日志
        val filterListBuffer = new ListBuffer[JSONObject]()

        //对分区的数据进行遍历
        for (jsonObj <- jsonObjIter) {
          //根据json对象获取日期
          val dt: String = jsonObj.getString("dt")
          //获取设备id
          val mid: String = jsonObj.getJSONObject("common").getString("mid")
          //拼接操作redis的key
          var dauKey = "dau: " + dt
          val isFirst: lang.Long = jedis.sadd(dauKey, mid)
          //设置key的失效时间
          if (jedis.ttl(dauKey) < 0) {
            jedis.expire(dauKey, 3600 * 24)
          }
          if (isFirst == 1L) {
            //首次登录
            filterListBuffer.append(jsonObj)
          }
        }

        jedis.close()

        filterListBuffer.toIterator
      }
    }
    //filterDStream.count().print()
    // 将数据批量保存到ES中
    filterDStream.foreachRDD(
      rdd => {// 以分区为单位处理
        rdd.foreachPartition{
          jsonObjItr => {
            val dauInfoList: List[(String, DauInfo)] = jsonObjItr.map {
              jsonObj => {
                //每次处理的是一个 json 对象 将 json 对象封装为样例类
                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                val dauInfo = DauInfo(
                  commonJsonObj.getString("mid"),
                  commonJsonObj.getString("uid"),
                  commonJsonObj.getString("ar"),
                  commonJsonObj.getString("ch"),
                  commonJsonObj.getString("vc"),
                  jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  "00", //分钟我们前面没有转换，默认 00
                  jsonObj.getLong("ts")
                )
                (dauInfo.mid, dauInfo)
              }
            }.toList
            dauInfoList

            // 将数据批量保存到ES中
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(dauInfoList, "gmall0523_dau_info_" + dt)
          }
        }
        // 提交偏移量到Redis中
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
