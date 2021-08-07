package com.nanfeng.gmall.realtime.ads

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.nanfeng.gmall.realtime.bean.OrderWide
import com.nanfeng.gmall.realtime.util.{MyKafkaUtil, OffsetManagerMySQL}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

import scala.collection.mutable.ListBuffer

/**
  * @author: nanfeng
  * @date: 2021/8/5 8:39
  * @description: 品牌统计应用程序
  */
object TrademarkStatApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName(this.getClass.getName)
    val ssc = new StreamingContext(conf, Seconds(5))

    val groupId = "ads_trademark_stat_group"
    val topic = "dws_order_wide"

    /*********************************************************************************
      * 1、数据初始化
      *******************************************************************************/
    // 从Mysql读取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerMySQL.getOffset(topic, groupId)
    // 加载kafka数据流
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0){
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    }else {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    // 读取本批次偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetrangesDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      consumerRecord => {
        offsetRanges = consumerRecord.asInstanceOf[HasOffsetRanges].offsetRanges

        consumerRecord
      }
    }
    // 转换数据结构
    val orderWideDStream: DStream[OrderWide] = offsetrangesDStream.map {
      consumerRecord => {
        val jsonStr: String = consumerRecord.value()
        val orderWide: OrderWide = JSON.parseObject(jsonStr, classOf[OrderWide])
        orderWide
      }
    }

    /*********************************************************************************
      * 2、对数据进行聚合处理,然后保存到MySQL
      *******************************************************************************/
    //orderWideDStream.print(1000)
    val orderWideWithAmountDStream: DStream[(String, Double)] = orderWideDStream.map {
      orderWide => {
        (orderWide.tm_id + "_" + orderWide.tm_name, Math.round((orderWide.final_detail_amount) * 100D) / 100D)
      }
    }

    val reduceDStream: DStream[(String, Double)] = orderWideWithAmountDStream.reduceByKey(_+_)

    //reduceDStream.print(1000)

    // 将数据保存到Mysql中 方案一 单条插入，一般不用
    /*reduceDStream.foreachRDD{
      rdd => {
        // 为了避免分布式事务，把ex的数据提取到driver中
        val tmSumArr: Array[(String, Double)] = rdd.collect()
        if (tmSumArr != null & tmSumArr.size > 0) {
          DBs.setup()
          DB.localTx {
            implicit session => {
              // 写入结算价结果数据
              val formator = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              for ((tm, amount) <- tmSumArr) {
                val statTime: String = formator.format(new Date())
                val tmArr: Array[String] = tm.split("_")
                val tmId: String = tmArr(0)
                val tmName: String = tmArr(1)
                val amountRound: Double = Math.round(amount * 100D)/100D
                println("数据写入执行")
                SQL("insert into trademark_amount_stat(stat_time, trademark_id, trademark_name, amount) values(?,?,?,?)")
                  .bind(statTime, tmId, tmName, amountRound)
                  .update()
                  .apply()
              }
              throw new RuntimeException("测试异常")
              // 写入偏移量
              for (offsetRange <- offsetRanges) {
                val partitionId: Int = offsetRange.partition
                val untilOffset: Long = offsetRange.untilOffset
                println("偏移量提交执行")
                SQL("replace into offset_0523 values(?,?,?,?)")
                  .bind(groupId, topic, partitionId, untilOffset)
                  .update()
                  .apply()
              }
            }
          }
        }
      }
    }*/
    // 将数据保存到Mysql中 方案二 批量插入
    reduceDStream.foreachRDD{
      rdd => {
        // 为了避免分布式事务，把ex的数据提取到Driver中执行。
        // 因为做了聚合，所以可以直接提取executor的数据聚合到driver端
        val tmSumArr: Array[(String, Double)] = rdd.collect()
        if (tmSumArr != null && tmSumArr.size > 0){
          DBs.setup()
          DB.localTx{
            implicit session => {
              // 写入计算结果
              val formator = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              val dateTime: String = formator.format(new Date())
              val batchParamList: ListBuffer[Seq[Any]] = ListBuffer[Seq[Any]]()
              for ((tm, amount) <- tmSumArr) {
                val amountRound: Double = Math.round(amount * 100D) / 100D
                val tmArr: Array[String] = tm.split("_")
                val tmId: String = tmArr(0)
                val tmName: String = tmArr(1)
                batchParamList.append(Seq(dateTime, tmId, tmName, amountRound))
              }
              //数据集合作为多个可变参数的方法，其参数要加：_*
              SQL("insert into trademark_amount_stat(stat_time, trademark_id, trademark_name, amount) values(?,?,?,?)")
                .batch(batchParamList.toSeq:_*)
                .apply()

              //throw new RuntimeException("测试异常")
              //写入偏移量
              println("********************** 偏移量保存开始 ********************")
              for (offsetRange <- offsetRanges) {
                val partitionId: Int = offsetRange.partition
                val fromOffset: Long = offsetRange.fromOffset
                val untilOffset: Long = offsetRange.untilOffset
                SQL("replace into offset_0523 values(?,?,?,?)")
                  .bind(groupId, topic, partitionId, untilOffset)
                  .update()
                  .apply()
                println("保存偏移量：" + topic + " - " + partitionId + ": " + fromOffset +  " -> " + untilOffset)
              }
              println("********************** 偏移量保存结束 ********************")
            }
          }
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
