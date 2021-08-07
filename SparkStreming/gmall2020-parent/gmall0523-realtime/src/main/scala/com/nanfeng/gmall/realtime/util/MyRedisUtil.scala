package com.nanfeng.gmall.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * @author: nanfeng
  * @date: 2021/7/20 20:53
  * @description:
  */
object MyRedisUtil {
  // 定义连接池对象
  private var jedisPool: JedisPool = null

  // 创建JedisPool连接池对象
  def build() = {
    val config = MyPropertiesUtil.load("config.properties")
    val host = config.getProperty("redis.host")
    val port = config.getProperty("redis.port")
    val jedisPoolConfig = new JedisPoolConfig()
    jedisPoolConfig.setMaxTotal(100) //最大连接数
    jedisPoolConfig.setMaxIdle(20) //最大空闲
    jedisPoolConfig.setMinIdle(20) //最小空闲
    jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
    jedisPoolConfig.setMaxWaitMillis(5000)//忙碌时等待时长 毫秒
    jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

    jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
  }

  // 获取Jedis客户端
  def getJedisClient() = {
    if (jedisPool == null) {
      build()
    }
    jedisPool.getResource
  }

  def main(args: Array[String]): Unit = {
    val jedisClient: Jedis = getJedisClient()
    println(jedisClient.ping())
    jedisClient.close()
  }
}
