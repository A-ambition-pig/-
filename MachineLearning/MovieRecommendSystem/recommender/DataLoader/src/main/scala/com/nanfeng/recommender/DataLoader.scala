package com.nanfeng.recommender

import java.net.InetAddress

import com.mongodb.MongoClientURI
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String,
                 directors: String)
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)

/**
  *
  * @param uri MongoDB连接
  * @param db MongoDB数据库
  */
case class MongoConfig(uri: String, db: String)

/**
  *
  * @param httpHosts http主机列表，逗号分隔
  * @param transportHosts transport主机列表
  * @param index 需要操作的索引
  * @param clustername 集群名称、默认
  */
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
    /**
      * mid, tags
      *
      * tags: tag1|tag2|tag3 ...
      */
    val newTag: DataFrame = tagDF.groupBy($"mid")
      .agg(concat_ws("|", collect_set($"tag")))
      .as("tags")
      .select("mid", "tags")
    // 需要将处理后的Tag数据，和Movie数据融合，产生新的Movie数据
    val movieWithTagsDF: DataFrame = movieDF.join(newTag, Seq("mid", "mid"), "left")


    // 声明了一个ES配置的隐式参数
    implicit val esConfig = ESConfig(config("es.httpHosts"),
      config("es.transportHosts"),
      config("es.index"),
      config("es.cluster.name"))

    // 需要将新的Movie数据保存到ES中
    storeDataInES( movieWithTagsDF)

    // 关闭Spark
    spark.stop()
  }

  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)
                        (implicit mongoConfig: MongoConfig) = {
    // 新建一个到MongoDB的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    // 如果MongoDB中有对应的数据库，那么应该删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    // 将当前数据写入到 MongoDB
    movieDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 对数据表建索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    // 关闭MongoDB的连接
    mongoClient.close()
  }

  def storeDataInES(movieDF: DataFrame)(implicit  esConfig: ESConfig) = {
    // 新建一个配置
    val settings: Settings = Settings.builder().put("cluster.name", esConfig.clustername).build()
    // 新建一个ES的客户端
    val esClient = new PreBuiltTransportClient(settings)
    // 需要将TransportHosts添加到esClient中
    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    esConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host: String, port: String) => {
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
      }
    }
    // 需要清除掉ES中遗留的数据
    if (esClient.admin().indices()
      .exists(new IndicesExistsRequest(esConfig.index)).actionGet().isExists){
      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
    }

    esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))

    // 将数据写入到ES中
    movieDF
      .write
      .option("es.nodes", esConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(esConfig.index + "/" + ES_MOVIE_INDEX)
  }
}
