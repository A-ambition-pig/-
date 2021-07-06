package com.nanfeng.offline

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created with IntelliJ IDEA.
  * User: hspcadmin
  * Date: 2021/7/6
  * Time: 16:30
  * Description: 
  */
// 基于评分数据的LFM，只需要rating数据
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)

object ALSTrainer {

  // 定义表名和常量
  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.core" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //创建 SparkConf 配置
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setMaster("ALSTrainer")
    //创建 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载评分数据
    val ratingRDD = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => Rating(rating.uid, rating.mid, rating.score)) // 转换成rdd，去掉时间戳
      .cache()

    // 随机切分数据集，生成训练集和测试集
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD = splits(0)
    val testRDD = splits(1)

    // 模型参数选择，输出最优参数
    adjustALSParam(trainingRDD, testRDD)

    spark.stop()
  }

  def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]) = {
    val result = for ( rank <- Array(20, 50, 100); lambda <- Array(0.001, 0.001, 0.1))
      yield {
        val model = ALS.train(trainData, rank, 50, lambda)
        // 计算当前参数对应模型的RMSE，返回Double
        val rmse = getRMSE(model, testData)
        (rank, lambda, rmse)
      }
    // 控制台打印输出
    println(result.minBy(_._3))
  }

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]) = {
    // 计算预测评分
    val userProducts = data.map(item => (item.user, item.product))
    val predictRating = model.predict(userProducts)

    // 以uid， mid 作为外键，inner join 观测值，预测值
    val observed = data.map(item => ((item.user, item.product), item.rating))
    val predict = predictRating.map(item => ((item.user, item.product), item.rating))

    // 内连接，（uid， mid）,(实际值，预测值)
    val result = sqrt(observed.join(predict).map{
      case ((uid, mid), (actual, pre)) => {
        val err = actual - pre
        err * err
      }
    }.mean())
    result
  }
}
