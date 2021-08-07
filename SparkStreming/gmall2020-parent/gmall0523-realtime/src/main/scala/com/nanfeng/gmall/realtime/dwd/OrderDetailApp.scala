package com.nanfeng.gmall.realtime.dwd

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.nanfeng.gmall.realtime.bean.{OrderDetail, SkuInfo}
import com.nanfeng.gmall.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author: nanfeng
  * @date: 2021/8/1 20:07
  * @description:
  */
object OrderDetailApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf =
      new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))

    var topic = "ods_order_detail"
    var groupId = "order_detail_group"

    /****************************************************************
      * 1、从Kafka中获取数据
      ***************************************************************/
    // 获取偏移量
    val offsetMap: Map[TopicPartition, Long] =
      OffsetManagerUtil.getOffset(topic, groupId)

    // 获取数据
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (null != offsetMap && offsetMap.size > 0) {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // 获取当前批次获取偏移量情况
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] =
      recordDStream.transform { rdd =>
        {
          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
        }
      }

    // 结构转换
    val orderDetailDStream: DStream[OrderDetail] = offsetDStream.map { record =>
      {
        // 获取json格式字符串
        val jsonStr: String = record.value()
        val orderDetail: OrderDetail =
          JSON.parseObject(jsonStr, classOf[OrderDetail])
        orderDetail
      }
    }

    //orderDetailDStream.print(1000)
    /**
      * 关联商品维度
      * 关联品牌维度
      * 关联类别维度
      * 关联 SPU 维度
      *
      * 维度退化：对变化感知弱
      *
      * 关联维度数据
      */
    /****************************************************************
      * 2.关联维度表
      ***************************************************************/
    val orderDetailWithSkuDStream: DStream[OrderDetail] =
      orderDetailDStream.mapPartitions { orderDetailItr =>
        {
          val orderDetailList: List[OrderDetail] = orderDetailItr.toList

          //从订单明细中拿到所有的商品id
          val skuIdList: List[Long] = orderDetailList.map(_.sku_id)
          //根据商品id到Phoenix中查询出所有的商品
          val sql =
            s"select id, tm_id, spu_id, category3_id, tm_name, spu_name, category3_name from gmall0523_sku_info where id in ('${skuIdList
              .mkString("','")}')"
          val skuJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)

          //转换结构，jsonObject => SkuInfo样例类,Map
          val skuInfoMap: Map[String, SkuInfo] = skuJsonObjList.map {
            skuJsonObj =>
              {
                val skuInfo: SkuInfo =
                  JSON.toJavaObject(skuJsonObj, classOf[SkuInfo])
                (skuInfo.id, skuInfo)
              }
          }.toMap

          //遍历, 给订单明细赋值
          for (orderDetail <- orderDetailList) {
            val skuInfo: SkuInfo =
              skuInfoMap.getOrElse(orderDetail.sku_id.toString, null)

            if (skuInfo != null) {
              orderDetail.spu_id = skuInfo.spu_id.toLong
              orderDetail.tm_id = skuInfo.tm_id.toLong
              orderDetail.category3_id = skuInfo.category3_id.toLong
              orderDetail.spu_name = skuInfo.spu_name
              orderDetail.tm_name = skuInfo.tm_name
              orderDetail.category3_name = skuInfo.category3_name
            }
          }

          orderDetailList.toIterator
        }
      }

    //orderDetailWithSkuDStream.print(1000)
    //将关联后的订单明细宽表写入到kafka中
    orderDetailWithSkuDStream.foreachRDD{
      rdd => {
        rdd.foreach{
          orderDetail => {
            MyKafkaSink.send("dwd_order_detail",
              JSON.toJSONString(orderDetail, new SerializeConfig(true)))
          }
        }
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
