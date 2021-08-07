package com.nanfeng.gmall.realtime.dws

import java.lang
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.nanfeng.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.nanfeng.gmall.realtime.util.{MyKafkaSink, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * @author: nanfeng
  * @date: 2021/8/1 23:18
  * @description: 注意：如果程序的数据来源是Kafka，在程序中如果触发多次行动算子，进行缓存
  */
object OrderWideApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))

    // order_info
    val orderInfoTopic = "dwd_order_info"
    val orderInfoGroupId = "dws_order_info_group"

    // order_detail
    val orderDetailTopic = "dwd_order_detail"
    val orderDetailGroupId = "dws_order_detail_group"

    /*****************************************************************
      * 1、获取偏移量
      *****************************************************************/
    // order_info
    // 1)获取偏移量
    val orderInfoOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderInfoTopic, orderInfoGroupId)
    var orderInfoRecordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffsetMap != null && orderInfoOffsetMap.size > 0){
      orderInfoRecordDStream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoOffsetMap, orderInfoGroupId)
    }else {
      orderInfoRecordDStream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoGroupId)
    }

    // 2）本批次偏移量
    var infoOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoOffsetDStream: DStream[ConsumerRecord[String, String]] = orderInfoRecordDStream.transform {
      rdd => {
        infoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    // 3）结构转换 jsonStr => 样例类对象
    val orderInfoDStream: DStream[OrderInfo] = orderInfoOffsetDStream.map {
      consumerRecord => {
        val jsonStr: String = consumerRecord.value()
        val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
        orderInfo
      }
    }

    // order_detail
    // 1)获取偏移量
    val orderDetailOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderDetailTopic, orderDetailGroupId)
    var orderDetailRecordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderDetailOffsetMap != null && orderDetailOffsetMap.size > 0) {
      orderDetailRecordDStream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailOffsetMap, orderDetailGroupId)
    }else {
      orderDetailRecordDStream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailGroupId)
    }

    // 2)拿到本批次偏移量
    var detailOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderDetailOffsetDStream: DStream[ConsumerRecord[String, String]] = orderDetailRecordDStream.transform {
      rdd => {
        detailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        rdd
      }
    }
    // 3)转换结构 jsonStr => 样例类对象
    val orderDetailDStream: DStream[OrderDetail] = orderDetailOffsetDStream.map {
      consumerRecord => {
        val jsonStr: String = consumerRecord.value()
        val orderDetail: OrderDetail = JSON.parseObject(jsonStr, classOf[OrderDetail])
        orderDetail
      }
    }

    //orderInfoDStream.print(1000)
    //orderDetailDStream.print(1000)
    /*****************************************************************
      * 2、双流join
      *****************************************************************/
    // 双流join
    // 1、开窗，设置窗口大小50s、滑动步长5s
    val orderInfoWindowDStream: DStream[OrderInfo] = orderInfoDStream.window(Seconds(50), Seconds(5))
    val orderDetailWindowDStream: DStream[OrderDetail] = orderDetailDStream.window(Seconds(50), Seconds(5))

    // 2、join => (orderID, (orderInfo, orderDetail))
    val orderInfoWithKeyDStream: DStream[(Long, OrderInfo)] = orderInfoWindowDStream.map(orderInfo=>(orderInfo.id, orderInfo))
    val orderDetailWithKeyDStream: DStream[(Long, OrderDetail)] = orderDetailWindowDStream.map(orderDetail=>(orderDetail.order_id, orderDetail))
    val joinedDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDStream.join(orderDetailWithKeyDStream, 4)


    // 3、去重，redis type:set  key:order_join:[orderId] value:orderDetailId expire:600
    // 以分区为单位，sadd 返回0 过滤
    val orderWideDStream: DStream[OrderWide] = joinedDStream.mapPartitions {
      tupleItr => {
        val tupleList: List[(Long, (OrderInfo, OrderDetail))] = tupleItr.toList

        // 宽表List
        val orderWideList: ListBuffer[OrderWide] = ListBuffer[OrderWide]()

        // 连接Redis
        val jedis: Jedis = MyRedisUtil.getJedisClient()

        for ((orderId, (orderInfo, orderDetail)) <- tupleList) {
          val key = "order_join:" + orderId
          val ifNotExisted: lang.Long = jedis.sadd(key, orderDetail.id.toString)
          jedis.expire(key, 600)

          //合并宽表
          if (ifNotExisted == 1L) {
            orderWideList.append(new OrderWide(orderInfo, orderDetail))
          }
        }

        jedis.close()
        orderWideList.toIterator
      }
    }

    //orderWideDStream.print(1000)
    /*****************************************************************
      * 3、实付分摊
      *****************************************************************/
    val orderWideWithSplitDStream: DStream[OrderWide] = orderWideDStream.mapPartitions {
      orderWideItr => {
        val orderWideList: List[OrderWide] = orderWideItr.toList

        // 获取Jedis连接
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        // 对订单宽表进行遍历,获取明细累加和，以及实付分摊累加和
        // 明细累加和：key: order_origin_sum:[order_id]
        // 实付分摊累加和：key: order_split_sum:[order_id]
        for (orderWide <- orderWideList) {
          // 获取明细累加和
          val originSumKey = "order_origin_sum:" + orderWide.order_id
          var orderOriginSum: Double = 0d
          val orderOriginSumStr = jedis.get(originSumKey)
          // 注意：从Redis中获取字符串，都要做非空判断
          if (orderOriginSumStr != null && orderOriginSumStr.size > 0) {
            orderOriginSum = orderOriginSumStr.toDouble
          }

          // 获取实付分摊累加和
          val splitSumKey: String = "order_split_sum:" + orderWide.order_id
          var orderSplitSum: Double = 0d
          val orderSplitSumStr: String = jedis.get(splitSumKey)
          if (orderSplitSumStr != null && orderSplitSumStr.size > 0) {
            orderSplitSum = orderSplitSumStr.toDouble
          }

          // 判断是否是最后一条，计算实付分摊
          val detailAmount = orderWide.sku_price * orderWide.sku_num
          if (detailAmount == orderWide.original_total_amount - orderOriginSum) {
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount - orderSplitSum) * 100d) / 100d
          } else {
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount * detailAmount / orderWide.original_total_amount) * 100d) / 100d
          }

          // 更新Redis
          // 更新实付分摊金额
          val newOrderSplitSum: String = (orderWide.final_detail_amount + orderSplitSum).toString
          jedis.setex(splitSumKey, 600, newOrderSplitSum)
          //更新明细金额
          val newOrderOriginSum: String = (detailAmount + orderOriginSum).toString
          jedis.setex(originSumKey, 600, newOrderOriginSum)

      }

        jedis.close()

        orderWideList.toIterator
      }
    }
    /*orderWideWithSplitDStream.map{
      orderWide => {
        JSON.toJSONString(orderWide, new SerializeConfig(true))
      }
    }.print(1000)*/
    //orderWideWithSplitDStream.print(1000)

    // 向ClickHouse中保存数据
    // 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().appName("spark_sql_orderWide").getOrCreate()
    import spark.implicits._

    //spark.read.format("").load()
    // 对DStream的RDD进行处理
    orderWideWithSplitDStream.foreachRDD{
      rdd => {
        rdd.cache()

        val df: DataFrame = rdd.toDF()
        df.write
          .mode(SaveMode.Append)
          .option("batchsize", "100")
          .option("isolationLevel", "NONE") // 设置事务
          .option("numPartitions", "4") // 设置并发
          .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
          .jdbc("jdbc:clickhouse://master:8123/default","t_order_wide_0523",new Properties())

        // 将数据写回到Kafka_dws_order_wide
        rdd.foreach{
          orderWide => {
            MyKafkaSink.send("dws_order_wide", JSON.toJSONString(orderWide, new SerializeConfig(true)))
          }
        }

        // 提交偏移量
        OffsetManagerUtil.saveOffset(orderInfoTopic, orderInfoGroupId, infoOffsetRanges)
        OffsetManagerUtil.saveOffset(orderDetailTopic, orderDetailGroupId, detailOffsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
