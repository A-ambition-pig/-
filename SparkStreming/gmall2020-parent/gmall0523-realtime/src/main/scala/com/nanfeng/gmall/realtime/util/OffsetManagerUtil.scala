package com.nanfeng.gmall.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

/**
  * @author: nanfeng
  * @date: 2021/7/25 11:03
  * @description:
  */
object OffsetManagerUtil {

  // 从Redis中获取偏移量 type:hash key: offset:topic:groupId field:partition value:偏移量
  def getOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    val jedis: Jedis = MyRedisUtil.getJedisClient()

    // 拼接key offset:topic:groupId
    var offsetKey = "offset:" + topic + ":" + groupId
    // 获取当前消费者组消费主题，以及对应分区偏移量
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)

    // 用完就可以关
    jedis.close()

    // 将javaMap转换为scalaMap
    import scala.collection.JavaConverters._
    val resultMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partition, offset) => {
        println("读取分区偏移量： " + partition + ":" + offset)
        //Map[TopicPartition, Long]
        (new TopicPartition(topic, partition.toInt), offset.toLong)
      }
    }.toMap

    resultMap
  }

  // 将偏移量信息保存到Redis中
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    // 拼接redis中操作偏移量的key
    var offsetKey = "offset:" + topic + ":" + groupId
    // 定义java的map集合，存放每个分区的偏移量
    val offsetMap = new util.HashMap[String, String]()
    // offsetMap赋值
    println("************************* 本次偏移量保存开始 ******************************")
    for (offsetRange <- offsetRanges) {
      val partitionId: Int = offsetRange.partition
      val fromOffset: Long = offsetRange.fromOffset
      val untilOffset: Long = offsetRange.untilOffset
      offsetMap.put(partitionId.toString, untilOffset.toString)
      println("保存分区" + partitionId + "：" + fromOffset + "-------->" + untilOffset)
    }
    println("************************* 本次偏移量保存结束 ******************************")

    val jedis: Jedis = MyRedisUtil.getJedisClient()

    jedis.hmset(offsetKey, offsetMap)

    jedis.close()
  }

}
