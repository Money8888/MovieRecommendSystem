package com.learning.statistics

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 基于统计的推荐模块
 * 包括
 * 历史最热、最近最热、电影评分平均、每个类别优质电影等四个模块
 * 技术采用mongodb查询数据，然后结果写回mongodb
 *
 */
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

// 基于类别统计
// 基准数据类
case class Recommendation(mid: Int, score: Double)
// 类别统计数据类
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])

case class MongoConfig(uri: String, db: String)

object StatisticsRecommender {

  // 读取的集合名
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  // 存入的统计集合名
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {
    // 定义配置
    val config = Map(
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 定义spark环境
    val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 读取数据

    // 配置需要隐式转换的包
    import spark.implicits._

    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    val movieDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      // 最后的结果是dataset
      .toDF()

    // 创建临时视图
    ratingDF.createOrReplaceTempView("ratings")

    // TODO: 四个统计模块
    // 1、历史热门统计
    val ratingMoreMovieDF = spark.sql("select mid, count(mid) as count from ratings group by mid order by count desc")
    storeDataInMongoDB(ratingMoreMovieDF, RATE_MORE_MOVIES)

    // 2、按月份的最近热门统计

    // 新建时间格式工具类方法，转化时间戳为yyyyMM格式
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // 注册udf到spark,new Date(x) x为毫秒值表示将时间戳转化成时间类型，以方便用simpleDateFormat转换成各种格式
    // 时间戳*1000超过int范围，乘以1000L自动转为Long，最后转化过后的yyyyMM为六位int整数
    // 过滤uid
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)
    val ratingOfYearMonth = spark.sql("select mid, changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")
    // 先按照月份分组，得到每月数据，再按每月不同的电影的电影id分组
    // 先按照月份降序，统计最近的月份，再根据每个月份中的不同电影的评分数降序
    // 为什么mongodb里面不是先按月降序再按count降序???
    val rateMoreRecentlyMovies = spark.sql("select mid, count(mid) as count, yearmonth from ratingOfMonth group by yearmonth, mid order by yearmonth desc, count desc")
    storeDataInMongoDB(rateMoreRecentlyMovies, RATE_MORE_RECENTLY_MOVIES)

    // 3、统计电影评分平均值
    val averageMoviesDF = spark.sql("select mid, avg(score) as avg from ratings group by mid")
    storeDataInMongoDB(averageMoviesDF, AVERAGE_MOVIES)

    // 4、每个类别优质电影

    // 枚举所有的类型名
    val genres = List("Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery" ,"Romance","Science","Tv","Thriller","War","Western")
    // 将电影df添加一列平均评分，默认没有评分的电影不推荐所以用内连接
    val movieWithScore = movieDF.join(averageMoviesDF, Seq("mid"))

    // 做类别与电影的笛卡尔积，目的是为了使每个电影的genres类型与类型对应，一个电影可能属于多个类型
    // 将list转成RDD
    val genresRDD = spark.sparkContext.makeRDD(genres)
    val genresTopMovies = genresRDD.cartesian(movieWithScore.rdd)
      // 过滤出符合一一对应的电影 类别信息
      .filter{
            // 判断是否包含类别
        case (genre, movieRow) => movieRow.getAs[String]("genres").toLowerCase.contains(genre.toLowerCase)
      }.map{
            // 转化成Recommendation基准数据类型
        case (genres, movieRow) => {
          (genres, ( movieRow.getAs[Int]("mid"), movieRow.getAs[Double]("avg")))
        }
      }.groupByKey() // 按照键genres分组
      .map{
        // 进行排序,items代表(int mid, double avg),对avg即第二个元素进行降序
        // 选取评分最大的十个
        case (genres, items) =>
          GenresRecommendation(genres, items.toList.sortWith(_._2 > _._2).take(10)
            // items.toList.sortWith(_._2 > _._2).take(10)之后仍然是一个集合，需要map构建成Recommendation类
        .map(item => Recommendation(item._1, item._2)))
      }.toDF()

    storeDataInMongoDB(genresTopMovies, GENRES_TOP_MOVIES)

  }

  def storeDataInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {

    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    // 如果mongodb中有对应的数据库应该删除
    // 类似于db.collection.drop
    mongoClient(mongoConfig.db)(collection_name).dropCollection()
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
