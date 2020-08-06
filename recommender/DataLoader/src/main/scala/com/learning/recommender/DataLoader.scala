package com.learning.recommender


import java.net.InetAddress

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

/**
 * 以“^”进行分割
 * 电影类
 * 以""进行的分割
 * mid 电影id
 * name 电影名称
 * descri:电影描述
 * timelong:时长
 * issue:发布时间
 * shoot:拍摄时间
 * language:语言
 * genres:类型
 * actors:演员
 * directors:导演
 **/
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                shoot: String, language: String, genres: String, actors: String, directors: String)

/**
 * 以","进行分割
 * 评分类
 * uid:用户id
 * mid：电影id
 * score：评分
 * timestamp： 评分时间戳
 */
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

/**
 * 标签类
 * 以","分割
 * uid:用户id
 * mid：电影id
 * tag：标签
 * timestamp： 评分时间戳
 */
case class Tags(uid: Int, mid: Int, tag: String, timestamp: Int)

// 配置mongo和ES
case class MongoConfig(uri: String, db: String)
case class ESConfig(httpHosts: String, transportHosts:String, index:String, clustername:String)

object DataLoader {

  val MOVIE_DATA_PATH = "D:\\JavaProject\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "D:\\JavaProject\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAGS_DATA_PATH = "D:\\JavaProject\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAGS_COLLECTION = "Tags"
  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {

    // 定义配置
    val config = Map(
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      // 集群的话用逗号隔开
      "es.httpHosts" -> "localhost:9200",
      "es.transportHosts" -> "localhost:9300",
      "es.index" -> "recommender",
      // 默认集群名字
      "es.cluster.name" -> "elasticsearch"
    )

    // 定义spark环境
    // val conf = new SparkConf().setAppName("DataLoader").setMaster("local[*]")
    val sparkConf = new SparkConf().setAppName("DataLoader").setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 加载数据

    // 配置需要隐式转换的包
    import spark.implicits._
    // 读入文件地址，转成RDD
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    // 转成DF
    val movieDF = movieRDD.map(
      item => {
        // 反斜杠\转义^，还要对反斜杠\本身转义
        val attr = item.split("\\^")
        // 最后一行为返回值
        Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim, attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
      }
    ).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(
      item => {
        val attr = item.split(",")
        Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
      }
    ).toDF()

    val tagsRDD = spark.sparkContext.textFile(TAGS_DATA_PATH)
    val tagsDF = tagsRDD.map(
      item => {
        val attr = item.split(",")
        Tags(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
      }
    ).toDF()

    // 保存数据入mongo

    // 新建隐式变量,表示三个都用同一个配置
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    // 三个DF存入mongodb
    // storeDataInMongoDB(movieDF, ratingDF, tagsDF)

    // 保存数据入ES

    // 需要将电影评分数据与电影数据合并，多个评分以"|"连接
    // 导入spark SQL函数
    import org.apache.spark.sql.functions._
    // $"mid" 选取列名，按照列名进行分组，聚合拼串所有的评分
    // concat_ws, 以指定的分割符进行拼串
    // collect_set 组成set集合
    val newTag = tagsDF.groupBy($"mid")
        .agg( concat_ws("|",collect_set($"tag"))
        // 指定列名
        .as("tags"))
        .select("mid", "tags")
    // 合并,按照电影id左连接
    val movieWithTagsDF = movieDF.join(newTag, Seq("mid", "mid"), "left")
//
//    // ES隐式对象
    implicit val esConfig = ESConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))
//    implicit val esConfig = ESConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))
//    // 需要将新的Movie数据保存到ES中
    storeDataInES(movieWithTagsDF)
    spark.close()
  }

  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagsDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    // 新建mongo客户端
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    // 如果mongodb中有对应的数据库应该删除
    // 类似于db.collection.drop
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAGS_COLLECTION).dropCollection()

    // 写入数据库
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
    tagsDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TAGS_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

  def storeDataInES(movieWithTagsDF: DataFrame)(implicit esConfig: ESConfig): Unit = {
    // 新建配置
    val settings: Settings = Settings.builder().put("cluster.name", esConfig.clustername).build()

    // 新建一个ES客户端
    val esClient = new PreBuiltTransportClient(settings)

    // 将所有的TransportHosts添加到esClient中
    // 使用正则表达式，过滤掉不为主机:port的字符串,.r表示正则
    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    esConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host: String, port: String) =>
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
    }

    // 清除掉ES已经存在的数据
    if(esClient.admin().indices().exists(new IndicesExistsRequest(esConfig.index)).actionGet().isExists){
      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
    }else{
      esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))
    }

    // 将数据写入ES中
    movieWithTagsDF
      .write
      .option("es.nodes", esConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(esConfig.index + "/" + ES_MOVIE_INDEX)
  }

}
