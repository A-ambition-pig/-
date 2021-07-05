package com.nanfeng.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created with IntelliJ IDEA.
  * User: hspcadmin
  * Date: 2021/7/5
  * Time: 17:16
  * Description: 
  */
case class Movie(mid: Int, name: String, descri: String,
                 timelong: String, issue: String, shoot: String,
                 language: String, genres: String, actors: String,
                 directors: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)
case class MongoConfig(uri: String, db: String)
// 定义一个基准推荐对象
case class Recommendation(mid: Int, score: Double)
// 定义电影类别top10推荐对象
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])

object StatisticsRecommender {

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  //统计的表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.core" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //创建 SparkConf 配置
    val sparkConf = new
        SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))
    //创建 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    //加入隐式转换
    import spark.implicits._

    // 从MongoDB里面加载数据
    val ratingDF = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    //创建一张名叫 ratings 的表
    ratingDF.createOrReplaceTempView("ratings")

    //TODO: 不同的统计推荐结果


    spark.stop()

  }
}
