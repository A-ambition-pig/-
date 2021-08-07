package com.nanfeng.gmall.realtime.util

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition

/**
  * @author: nanfeng
  * @date: 2021/8/4 21:48
  * @description: 从Mysql中读取偏移量的工具类
  */
object OffsetManagerMySQL {

  // 获取偏移量
  def getOffset(topic: String, consumerGroupId: String): Map[TopicPartition, Long] = {
    val sql = "select group_id, topic, topic_offset, partition_id from offset_0523 " +
      "where topic='" + topic + "' and group_id='" + consumerGroupId + "'"

    val jsonObjList: List[JSONObject] = MySQLUtil.queryList(sql)

    val topicPartitionMap: Map[TopicPartition, Long] = jsonObjList.map {
      jsonObj => {
        val topicPartition = new TopicPartition(topic, jsonObj.getIntValue("partition_id"))
        val offset: Long = jsonObj.getLongValue("topic_offset")
        (topicPartition, offset)
      }
    }.toMap

    topicPartitionMap
  }
}
