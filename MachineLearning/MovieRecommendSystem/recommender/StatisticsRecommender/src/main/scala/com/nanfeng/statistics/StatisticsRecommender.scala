package com.nanfeng.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

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
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
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

    // 1.根据所有历史评分数据，计算历史评分次数最多的电影
    // 统计所有历史数据中每个电影的评分个数
    val rateMoreMoviesDF = spark.sql(
      """
        |select mid, count(mid) as count
        |  from ratings
        | group by mid
      """.stripMargin)
    storeDFInMongoDB(rateMoreMoviesDF, RATE_MORE_MOVIES)

    // 2.根据评分，按月("yyyyMM")为单位计算最近时间的月份里面评分数最多的电影集合
    // 统计以月为单位的每个电影的评分数
    // mid, count, time
    // 创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // 注册一个UDF函数，用于转换时间
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date( x * 1000L)).toInt)
    // 将原来的Rating数据集中，时间进行转换
    val ratingOfYearMonth = spark.sql(
      """
        |select mid, score changeDate(timestamp) as yearOfMonth
        |  from ratings
      """.stripMargin)
    // 将新的数据集注册成一张表
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    val rateMoreRecentlyMovies = spark.sql(
      """
        |select mid, count(mid) as count, yearOfMonth
        |  from ratingOfMonth
        | group by yearOfMonth, mid
        | order by yearOfMonth desc, mid desc
      """.stripMargin)
    storeDFInMongoDB(rateMoreRecentlyMovies, RATE_MORE_RECENTLY_MOVIES)

    // 3.根据历史数据中所有用户对电影的评分，周期性的计算每个电影的平均得分
    // 统计每个电影的平均评分
    val averageMoviesDF = spark.sql(
      """
        |select mid, avg(score) as avg from ratings group by mid
      """.stripMargin)
    storeDFInMongoDB(averageMoviesDF, AVERAGE_MOVIES)

    // 4.根据提供的所有电影类别，分别计算每种类型的电影集合中评分最高的10个电影
    // 1) 按类型分组，根据评分排序，取top10
    val movieWithScore = movieDF.join(averageMoviesDF, Seq("mid"))
    //所有的电影类别
    val genres = List("Action","Adventure","Animation","Comedy","Crime",
        "Documentary","Drama","Family","Fantasy","Foreign",
        "History","Horror","Music","Mystery","Romance",
        "Science","Tv","Thriller","War","Western")
    // 将电影类别转换成RDD
    val genresRDD = spark.sparkContext.makeRDD(genres)

    // 计算电影类别Top10
    val genresTopMovies = genresRDD.cartesian(movieWithScore.rdd)
        .filter{
          // 过滤出包含某种类型的电影
          case (genre, row) => {
            row.getAs[String]("genres").toLowerCase().contains(genre.toLowerCase)
          }
        }
        .map{
          // 将整个数据集的数据量将减小，生成RDD[String, Iter[mid, avg])
          case (genre, row) => {
            (genre, (row.getAs[Int]("mid"), row.getAs[Double]("avg")))
          }
        }
        .groupByKey()
        .map{
          case (genre, items) => {
            GenresRecommendation(genre, items.toList.sortWith(_._2>_._2).take(10)
            .map(item => Recommendation(item._1, item._2)))
          }
        }.toDF

    storeDFInMongoDB(genresTopMovies, GENRES_TOP_MOVIES)

    spark.stop()

  }


  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
