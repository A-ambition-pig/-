package com.nanfeng.recommender

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String,
                 directors: String)
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)
case class MongoConfig(uri: String, db: String)
case class ESConfig(httpHosts: String, transportHosts: String, index: String, clustername: String)

object DataLoader {

  val MOVIE_DATA_PATH = "movies.csv"
  val RATING_DATA_PATH = "ratings.csv"
  val TAG_DATA_PATH = "tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "localhost:9200",
      "es.transportHosts" -> "localhost:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "elasticsearch"
    )
    // 创建一个SparkConf配置
    val sparkConf: SparkConf = new SparkConf().setAppName("DataLoader").setMaster(config("spark.cores"))
    // 创建一个SparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 在对DataFrame 和 DataSet进行操作，许多操作都需要这个包进行支持
    import spark.implicits._

    // 将Movie、Rating、Tag数据集加载进来
    val movieRDD: RDD[String] = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    // 将MovieRDD转换成DataFrame
    val movieDF: DataFrame = movieRDD.map(item => {
      val attr: Array[String] = item.split("\\^")
      Movie(attr(0).trim.toInt,
        attr(1).trim,
        attr(2).trim,
        attr(3).trim,
        attr(4).trim,
        attr(5).trim,
        attr(6).trim,
        attr(7).trim,
        attr(8).trim,
        attr(9).trim
      )
    }).toDF()

    val ratingRDD: RDD[String] = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF: DataFrame = ratingRDD.map(item => {
      val attr: Array[String] = item.split(",")
      Rating(attr(0).toInt,
        attr(1).toInt,
        attr(2).toDouble,
        attr(3).toInt)
    }).toDF()

    val tagRDD: RDD[String] = spark.sparkContext.textFile(TAG_DATA_PATH)
    val tagDF: DataFrame = tagRDD.map(item => {
      val attr: Array[String] = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2), attr(3).toInt)
    }).toDF()

    // 声明一个隐式的配置对象
    implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get, config.get("mongo.db").get)
    // 将数据保存到MongoDB中
    storeDataInMongoDB(movieDF, ratingDF, tagDF)

    import org.apache.spark.sql.functions._
    val newTag: DataFrame = tagDF.groupBy($"mid")
      .agg(concat_ws("|", collect_set($"tag")))
      .as("tags")
      .select("mid", "tags")
    // 需要将处理后的Tag数据，和Movie数据融合，产生新的Movie数据
    val movieWithTagsDF: DataFrame = movieDF.join(newTag, Seq("mid", "mid"), "left")


    // 声明了一个ES配置的隐式参数
    implicit val esConfig = ESConfig(config.get("es.httpHosts").get,
      config.get("es.transportHosts").get,
      config.get("es.index").get,
      config.get("es.cluster.name").get)

    // 需要将新的Movie数据保存到ES中
    storeDataInES( movieWithTagsDF)

    // 关闭Spark
    spark.stop()
  }

  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)
                        (implicit mongoConfig: MongoConfig) = {

  }

  def storeDataInES(movieDF: DataFrame)(implicit  esConfig: ESConfig) = {

  }
}
