package com.nanfeng.gmall.realtime.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.nanfeng.gmall.realtime.bean.{BaseTrademark, SkuInfo, SpuInfo}
import com.nanfeng.gmall.realtime.util.{MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}


/**
  * @author: nanfeng
  * @date: 2021/8/1 20:45
  * @description:
  */
object SkuInfoApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName(this.getClass.getName)
    val ssc = new StreamingContext(conf, Seconds(5))

    var topic = "ods_sku_info"
    var groupId = "dim_sku_info_group"

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

    // 结构转换
    val skuInfoDStream: DStream[SkuInfo] = offsetDStream.map {
      consumerRecord => {
        // 结构转换
        val jsonStr: String = consumerRecord.value()
        val skuInfo: SkuInfo = JSON.parseObject(jsonStr, classOf[SkuInfo])
        skuInfo
      }
    }

    /****************************************************************
      * 2、维度退化 将BaseTrademark、BaseCategory、SpuInfo、SkuInfo进行关联
      ***************************************************************/
    val skuInfoChangeDStream: DStream[SkuInfo] = skuInfoDStream.transform {
      skuInfo => {
        //tm_name
        val tmSql = "select id, tm_name from gmall0523_base_trademark"
        val tmList: List[JSONObject] = PhoenixUtil.queryList(tmSql)
        val tmMap: Map[String, JSONObject] = tmList.map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap

        //category3
        val category3Sql = "select id, name from gmall0523_base_category3"
        val category3List: List[JSONObject] = PhoenixUtil.queryList(category3Sql)
        val category3Map: Map[String, JSONObject] = category3List.map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap

        // spu
        val spuSql = "select id, spu_name from gmall0523_spu_info"
        val spuList: List[JSONObject] = PhoenixUtil.queryList(spuSql)
        val spuMap: Map[String, JSONObject] = spuList.map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap

        // 汇总到一个list 广播
        val dimList: List[Map[String, JSONObject]] = List[Map[String, JSONObject]](category3Map, tmMap, spuMap)
        val dimBC: Broadcast[List[Map[String, JSONObject]]] = ssc.sparkContext.broadcast(dimList)

        val skuInfoRDD: RDD[SkuInfo] = skuInfo.map {
          sku => {
            //ex
            val dimList: List[Map[String, JSONObject]] = dimBC.value //接收广播变量
            val category3Map: Map[String, JSONObject] = dimList(0)
            val tmMap: Map[String, JSONObject] = dimList(1)
            val spuMap: Map[String, JSONObject] = dimList(2)

            val category3JsonObj: JSONObject = category3Map.getOrElse(sku.category3_id, null)
            if (category3JsonObj != null) {
              sku.category3_name = category3JsonObj.getString("NAME")
            }

            val tmJsonObj: JSONObject = tmMap.getOrElse(sku.tm_id, null)
            if (tmJsonObj != null) {
              sku.tm_name = tmJsonObj.getString("TM_NAME")
            }

            val spuJsonObj: JSONObject = spuMap.getOrElse(sku.spu_id, null)
            if (spuJsonObj != null) {
              sku.spu_name = spuJsonObj.getString("SPU_NAME")
            }

            sku
          }
        }
        skuInfoRDD
      }
    }

    skuInfoChangeDStream.print(1000)



    /****************************************************************
      * 3、保存数据到Phoenix
      ***************************************************************/
    import org.apache.phoenix.spark._
    skuInfoChangeDStream.foreachRDD{
      skuInfo => {
        // 保存到HBase
        skuInfo.saveToPhoenix(
          "GMALL0523_SKU_INFO",
          Seq("ID", "SPU_ID","PRICE","SKU_NAME","TM_ID",
            "CATEGORY3_ID","CREATE_TIME","CATEGORY3_NAME","SPU_NAME","TM_NAME" ),
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
