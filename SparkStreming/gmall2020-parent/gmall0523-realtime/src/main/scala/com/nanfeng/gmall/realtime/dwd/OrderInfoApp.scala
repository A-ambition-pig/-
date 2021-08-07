package com.nanfeng.gmall.realtime.dwd

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.nanfeng.gmall.realtime.bean.{OrderInfo, ProvinceInfo, UserInfo, UserStatus}
import com.nanfeng.gmall.realtime.util._
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable

/**
  * @author: nanfeng
  * @date: 2021/7/27 20:06
  * @description:
  */
object OrderInfoApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BaseDBCanalApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))

    var topic = "ods_order_info"
    var groupId = "order_info_group"

    // ==========================1、从Kafka主题中读取数据=========================
    // Redis
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    var recordStream: InputDStream[ConsumerRecord[String, String]] = null
    if (null != offsetMap && offsetMap.size > 0) {
      recordStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      recordStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //2、获取当前批次处理的偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    // 3、转换结构
    // ConsumerRecord[k.v] => value: jsoNStr => OrderInfo
    val orderInfoDStream: DStream[OrderInfo] = offsetDStream.map {
      record => {
        // 获取json格式字符串
        val jsonStr: String = record.value()

        // 将json格式字符串转换为OrderInfo对象
        val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
        // 2021-07-27 14:20:30
        val createTime: String = orderInfo.create_time
        val createTimeArr: Array[String] = createTime.split(" ")
        orderInfo.create_date = createTimeArr(0)
        orderInfo.create_hour = createTimeArr(1).split(":")(0)
        orderInfo
      }
    }

    //OrderInfoDStream.print(100)
    // ==========================2、判断是否为首单===========================

    // 方案一 获取用户id 对于每条sql都要执行一次查询
    /* val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDStream.map {
       orderInfo => {
         // 获取用户id
         val userId: Long = orderInfo.user_id
         // 根据userId到Phoenix中查询是否已经下单过
         var sql = s"select user_id, if_consumed from user_status0523 where user_id = '${userId}'"
         val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
         if (null != userStatusList && userStatusList.size > 0) {
           orderInfo.if_first_order = "0"
         } else {
           orderInfo.if_first_order = "1"
         }
         orderInfo
       }
     }

     orderInfoWithFirstFlagDStream.print(100)*/

    // 方案 二 以分区为单位，将整个分区的数据拼接成一条sql执行查询
    // TODO 同一个采集周期，同一个用户下了多单 漏洞
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions {
      orderInfoItr => {
        //当前一个分区中所有的订单集合
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //获取当前分区中获取下订单的用户
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        //根据userId集合到Phoenix中查询
        // 坑1：字符串拼接
        var sql = s"select user_id, if_consumed from user_status0523 where user_id in ('${userIdList.mkString("','")}')"

        //执行sql从Phoenix中获取数据
        val userListStatus: List[JSONObject] = PhoenixUtil.queryList(sql)
        //获取消费过的userId
        // 坑2：大小写
        val consumedUserIdList: List[String] = userListStatus.map(_.getString("USER_ID"))
        for (orderInfo <- orderInfoList) {
          // 坑3：类型转换
          if (consumedUserIdList.contains(orderInfo.user_id.toString)) {
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }


        orderInfoList.toIterator
      }
    }

    // ========================4、同一个批次中，状态修正=========================
    /*
      同一个采集周期中，同一用户的最早的订单标记为首单，其它改为非首单
      1）同一采集周期中同一用户--------按用户分组（groupByKey）
      2）最早的订单---------排序，取最早（sortWith）
      3）标记为首单--------具体业务代码
      */
    // 结构转换
    val mapDStream: DStream[(Long, OrderInfo)] = orderInfoWithFirstFlagDStream.map(orderInfo => (orderInfo.user_id, orderInfo))
    // 根据用户id对数据进行分组
    val groupByKeyDStream: DStream[(Long, Iterable[OrderInfo])] = mapDStream.groupByKey()
    val orderInfoRealStream: DStream[OrderInfo] = groupByKeyDStream.flatMap {
      case (userId, orderInfoItr) => {
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        // 判断在一个采集周期中，用户是否下了多个订单
        if (null != orderInfoList && orderInfoList.size > 1) {
          // 如果下了多个订单，按照下单时间升序排序
          val sortedOrderInfoList: List[OrderInfo] = orderInfoList.sortWith {
            (orderInfo1, orderInfo2) => {
              orderInfo1.create_time < orderInfo2.create_time
            }
          }
          //sortedOrderInfoList(0) 取出集合中的第一个元素
          if (sortedOrderInfoList(0).if_first_order == "1") {
            //时间最早的订单首单保留为1，其它都设置为0，非首单
            for (i <- 1 to sortedOrderInfoList.size - 1) {
              sortedOrderInfoList(i) == "0"
            }
          }

          sortedOrderInfoList
        } else {
          orderInfoList
        }

      }
    }

    /** *****************************************************************
      * 5.和省份维度表进行关联
      * ****************************************************************/
    // 5.1 方案一：以分区为单位，对订单数据进行处理，和Phoenix中的订单进行关联
    /*val orderInfoWithProvinceDStream1: DStream[OrderInfo] = orderInfoRealStream.mapPartitions {
      // 转换结构
      orderInfoItr => {
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        // 从当前分区中订单对应的省份id
        val provinceIdList: List[Long] = orderInfoList.map(_.province_id)
        //根据省份id查询

        var sql = s"select id, name, area_code, iso_code from gmall0523_province_info where id in ('${provinceIdList.mkString("','")}')"
        val provinceInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val provinceInfoMap: Map[String, ProvinceInfo] = provinceInfoList.map {
          provinceJsonObj => {
            // TODO jsonObj => profinceInfoObj
            val provinceInfo: ProvinceInfo = JSON.toJavaObject(provinceJsonObj, classOf[ProvinceInfo])

            (provinceInfo.id, provinceInfo)
          }
        }.toMap

        // TODO 怎么把订单和省份关联起来
        // 对订单数据进行遍历，用省份id，取对应数据
        for (orderInfo <- orderInfoList) {
          val provinceInfo: ProvinceInfo = provinceInfoMap
            .getOrElse(orderInfo.province_id.toString, null)

          if (provinceInfo != null) {
            orderInfo.province_name = provinceInfo.name
            orderInfo.province_area_code = provinceInfo.area_code
            orderInfo.province_iso_code = provinceInfo.iso_code
          }
        }

        orderInfoList.toIterator
      }
    }*/

    //orderInfoWithProvinceDStream.print(1000)
    // 5.2 方案二 已采集周期为单位进行处理
    // 使用广播变量，在Driver端进行一次查询，分区越多效果越明显 前提：省份数据量较小
    val orderInfoWithProvinceDStream: DStream[OrderInfo] = orderInfoRealStream.transform {
      rdd => {
        var sql = "select id, name, area_code, iso_code from gmall0523_province_info"
        val provinceInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val provinceInfoMap: Map[String, ProvinceInfo] = provinceInfoList.map {
          provinceJsonObj => {
            // provicneJsonObj => provinceInfo
            val provinceInfo: ProvinceInfo = JSON.toJavaObject(provinceJsonObj, classOf[ProvinceInfo])
            (provinceInfo.id, provinceInfo)
          }
        }.toMap

        val provimceInfoCast: Broadcast[Map[String, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceInfoMap)

        val newRDD: RDD[OrderInfo] = rdd.map {
          orderInfo => {
            val provinceInfo: ProvinceInfo = provimceInfoCast.value.getOrElse(orderInfo.province_id.toString, null)

            if (provinceInfo != null) {
              orderInfo.province_name = provinceInfo.name
              orderInfo.province_area_code = provinceInfo.area_code
              orderInfo.province_iso_code = provinceInfo.iso_code
            }

            orderInfo
          }
        }

        newRDD
      }
    }
    //orderInfoWithProvinceDStream.print(1000)

    /** *****************************************************************
      * 6.和用户维度表进行关联
      * ****************************************************************/
    val orderInfoWithProvinceAndUserDStream: DStream[OrderInfo] = orderInfoWithProvinceDStream.mapPartitions {
      orderInfoItr => {
        // 转换为list集合
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        // 获得所有用户id
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        // 拼接查询sql
        var sql = "select id, user_level, birthday, gender, age_group, gender_name from gmall0523_user_info " +
          s"where id in ('${userIdList.mkString("','")}')"
        // 获取当前分区中所有的下单用户
        val userInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
        // 进行结构转换，userInfoJsonObj => UserInfo
        val userMap: Map[String, UserInfo] = userInfoList.map {
          userJsonObj => {
            val userInfo: UserInfo = JSON.toJavaObject(userJsonObj, classOf[UserInfo])
            (userInfo.id, userInfo)
          }
        }.toMap

        // 对订单记录进行遍历.对用户相关信息进行填充
        for (orderInfo <- orderInfoList) {
          val userInfo: UserInfo = userMap
            .getOrElse(orderInfo.user_id.toString, null)

          if (userInfo != null) {
            orderInfo.user_age_group = userInfo.age_group
            orderInfo.user_gender = userInfo.gender_name
          }
        }

        orderInfoList.toIterator
      }
    }

    //orderInfoWithProvinceAndUserDStream.print(1000)

    // ===============================3、维护首单用户状态============================
    // 如果当前用户为首单用户（第一次消费），进行首单标记过后，应该将用户的消费状态保存到Hbase中
    // 下次这个用户再下单时，就不是首单了
    import org.apache.phoenix.spark._
    orderInfoWithProvinceAndUserDStream.foreachRDD {
      rdd => {
        // 优化
        rdd.cache()

        val userStatusRDD: RDD[UserStatus] = rdd.filter(_.if_first_order == "1") // 将首单用户提出来
          .map {
          orderInfo => {
            UserStatus(orderInfo.user_id.toString, "1")
          }
        }
        // 3.1保存到Hbase中
        userStatusRDD.saveToPhoenix(
            "USER_STATUS0523",
            Seq("USER_ID", "IF_CONSUMED"),
            new Configuration,
            Some("master,slave1,slave2:2181")
        )

        // 3.2保存订单数据到es中
        // 以分区为单位，对rdd进行处理
        rdd.foreachPartition{
          orderInfoItr => {
            // 结构转换
            val orderInfoList: List[(String, OrderInfo)] = orderInfoItr.toList.map {
              orderInfo => {
                (orderInfo.id.toString, orderInfo)
              }
            }

            // 保存到ES中
//            val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
//            MyESUtil.bulkInsert(orderInfoList, "gmall0523_order_info_" + dateStr)

            //3.4将订单信息推回kafka进入下一层处理   topic：dwd_order_info
            for ((orderInfoId, orderInfo) <- orderInfoList) {
              MyKafkaSink.send("dwd_order_info",
                JSON.toJSONString(orderInfo, new SerializeConfig(true)))
            }
          }
        }

        // 3.3保存偏移量
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
