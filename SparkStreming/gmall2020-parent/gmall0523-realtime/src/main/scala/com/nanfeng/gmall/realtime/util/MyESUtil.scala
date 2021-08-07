package com.nanfeng.gmall.realtime.util

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core._
import java.util

import com.nanfeng.gmall.realtime.bean.DauInfo
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, QueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder

/**
  * @author: nanfeng
  * @date: 2021/7/19 22:42
  * @description:
  */
object MyESUtil {


  //声明Jest客户端工厂
  private var jestFactory: JestClientFactory = null



  //提供获取Jest客户端的方法
  def getJestClient(): JestClient = {
    if (null == jestFactory){
      //创建Jest客户端工厂对象
      build()
    }
    jestFactory.getObject
  }

  def build() = {
    jestFactory = new JestClientFactory()
    jestFactory.setHttpClientConfig(new HttpClientConfig
    .Builder("http://master:9200")
    .multiThreaded(true)
    .maxTotalConnection(20)
    .connTimeout(10000)
    .readTimeout(1000)
    .build())
  }

  // 将插入对象封装为样例类对象
  def putIndex2() = {
    val client: JestClient = getJestClient()


    var actorList = new util.ArrayList[util.Map[String, Any]]()
    val actorMap1 = new util.HashMap[String, Any]()
    actorMap1.put("id", 66)
    actorMap1.put("name", "段誉")
    actorList.add(actorMap1)
    //封装样例类对象
    val movie = Movie(300, "天龙八部", 9.0f, actorList)

    // 创建Action实现类 ====> Index
    val index: Index = new Index.Builder(movie)
      .index("movie_index_5")
      .`type`("movie")
      .id("2")
      .build()

    client.execute(index)

    client.close()
  }

  //向ES中插入单跳数据 方式1 以json传递对象
  def putIndex1() = {
    //获取客户端连接
    val jestClient: JestClient = getJestClient()

    var source: String =
      """
        |{
        |  "id": 200,
        |  "name": "incident red sea",
        |  "doubanScore": 8.0,
        |  "actorList": [
        |    {
        |      "id": 4,
        |      "name": "zhang cui shan"
        |    }
        |  ]
        |}
      """.stripMargin
    //创建插入类 Index
    val index: Index = new Index.Builder(source)
      .index("movie_index_5")
      .`type`("movie")
      .id("1")
      .build()

    //通过客户端对象操作ES execute参数为Action类型, index为实现类
    val result: DocumentResult = jestClient.execute(index)

    //关闭连接
    jestClient.close()
  }

  //根据文档id， 从ES中查询出一条记录
  def queryIndexById() = {
    val jestClient: JestClient = getJestClient()

    val get: Get = new Get.Builder("movie_index_5", "2").build()
    val result: DocumentResult = jestClient.execute(get)
    println(result.getJsonString)

    jestClient.close()
  }

  //根据指定查询条件，从ES中查询多个文档 方式1
  def queryIndexByCondition1() = {
    val client: JestClient = getJestClient()

    val query: String =
      """
        |{
        |  "query": {
        |    "bool": {
        |      "must": [
        |        {
        |          "match": {
        |            "name": "天龙"
        |          }
        |        }
        |      ],
        |      "filter": [
        |        {
        |          "term": {
        |            "actorList.name.keyword": "段誉"
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "from": 0,
        |  "size": 20,
        |  "sort": [
        |    {
        |      "doubanScore": {
        |        "order": "desc"
        |      }
        |    }
        |  ],
        |  "highlight": {
        |    "fields": {
        |      "name": {}
        |    }
        |  }
        |}
      """.stripMargin

    //封装Search对象
    val search: Search = new Search.Builder(query)
      .addIndex("movie_index_5")
      .build()
    val result: SearchResult = client.execute(search)
    val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
    //将java的List转换为json的List
    import scala.collection.JavaConverters._
    val resList1: List[util.Map[String, Any]] = list.asScala.map(_.source).toList

    println(resList1.mkString("\n"))

    client.close()
  }

  //根据指定查询条件，从ES中查询多个文档 方式1
  def queryIndexByCondition2() = {
    val client: JestClient = getJestClient()

    //用于构建查询的json格式字符串
    val searchSourceBuilder = new SearchSourceBuilder()
    val boolQueryBuilder = new BoolQueryBuilder()
    boolQueryBuilder.must(new MatchQueryBuilder("name", "天龙"))
    boolQueryBuilder.filter(new TermQueryBuilder("actorList.name.keyword", "段誉"))

    searchSourceBuilder.query(boolQueryBuilder)
    searchSourceBuilder.from(0)
    searchSourceBuilder.size(10)
    searchSourceBuilder.sort("doubanScore", SortOrder.ASC)
    searchSourceBuilder.highlighter(new HighlightBuilder().field("name"))
    val query: String = searchSourceBuilder.toString

    //println(query)
    val search: Search = new Search.Builder(query).addIndex("movie_index_5").build()
    val result: SearchResult = client.execute(search)
    val resList: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
    import scala.collection.JavaConverters._
    val list: List[util.Map[String, Any]] = resList.asScala.map(_.source).toList

    println(list.mkString("\n"))
    //封装Search对象
//    val search: Search = new Search.Builder(query)
//      .addIndex("movie_index_5")
//      .build()
//    val result: SearchResult = client.execute(search)
//    val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
//    //将java的List转换为json的List
//    import scala.collection.JavaConverters._
//    val resList1: List[util.Map[String, Any]] = list.asScala.map(_.source).toList
//
//    println(resList1.mkString("\n"))

    client.close()
  }

  def main(args: Array[String]): Unit = {
    //putIndex2()
    //queryIndexById()
    queryIndexByCondition2()
  }

  /**
    * 向ES中批量插入数据
    * @param infoList
    * @param indexName
    */
  def bulkInsert(infoList: List[(String, Any)], indexName: String): Unit = {
    if (null != infoList && infoList.size != 0) {

      val jestClient: JestClient = getJestClient()

      val bulkBuilder = new Bulk.Builder()
      for ((id, dauInfo) <- infoList) {
        val index: Index = new Index.Builder(dauInfo)
          .index(indexName)
          .id(id)
          .`type`("_doc")
          .build()

        bulkBuilder.addAction(index)
      }

      //创建批量操作对象
      val bulk: Bulk = bulkBuilder.build()
      val bulkResult: BulkResult = jestClient.execute(bulk)
      println("向ES中插入了 " + bulkResult.getItems.size() + " 条数据")

      jestClient.close()
    }
  }
}



case class Movie(id: Long, name: String, doubanScore: Float, actorList: util.List[util.Map[String, Any]])
