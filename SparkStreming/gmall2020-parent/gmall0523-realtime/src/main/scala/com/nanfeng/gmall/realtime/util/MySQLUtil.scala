package com.nanfeng.gmall.realtime.util


import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * @author: nanfeng
  * @date: 2021/8/4 21:43
  * @description:
  */
object MySQLUtil {
  def main(args: Array[String]): Unit = {
    var sql = "select * from  offset_0523"
    val rs: List[JSONObject] = queryList(sql)
    println(rs)
  }

  def queryList(sql: String): List[JSONObject] = {
    val rsList = new ListBuffer[JSONObject]
    // 1、注册驱动
    Class.forName("com.mysql.jdbc.Driver")

    // 2、建立连接
    val conn: Connection = DriverManager.getConnection(
      "jdbc:mysql://slave2:3306/gmall0523_rs?characterEncoding=utf-8&useSSL=false",
      "root",
      "12345678")

    // 3、创建数据库操作对象
    val ps: PreparedStatement = conn.prepareStatement(sql)

    // 4、执行SQL语句
    val rs: ResultSet = ps.executeQuery()
    val rsMetaData: ResultSetMetaData = rs.getMetaData


    // 5、处理结果集
    // TODO
    while (rs.next()) {
      val userStatusJsonObj = new JSONObject()

      //{"user_id":"ww","if_consumered":"1"}
      for (i <- 1 to rsMetaData.getColumnCount) {
        userStatusJsonObj.put(rsMetaData.getColumnName(i), rs.getObject(i))
      }

      rsList.append(userStatusJsonObj)

    }


    // 6、释放资源
    rs.close()
    ps.close()
    conn.close()

    rsList.toList
  }
}
