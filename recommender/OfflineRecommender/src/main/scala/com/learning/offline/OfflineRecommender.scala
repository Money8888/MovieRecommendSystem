package com.learning.offline

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix


/**
 * 基于隐语义模型的用户评分推荐列表
 * 电影的相关性矩阵
 */
// Rating在spark-mllib中也有相关类
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class Recommendation(mid: Int, score: Double)
// 类别统计数据类
case class UserRecs(uid: Int, recs: Seq[Recommendation])
// 相关性列表
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

case class MongoConfig(uri: String, db: String)

object OfflineRecommender {

  val MONGODB_RATING_COLLECTION = "Rating"

  // 推荐表的名称
  val USER_RECS_COLLECTION = "UserRecs"
  val MOVIE_RECS_COLLECTION = "MovieRecs"
  // 最大用户推荐数
  val USER_MAX_RECOMMENDATION = 20
  // 最小电影相关系数值
  val CORR_MIN_VALUE = 0.6
  // ALS模型参数, rank指的用户矩阵的列数*电影矩阵的行数
  val (rank, iterations, lambda) = (60, 5, 0.01)


  def main(args: Array[String]): Unit = {
    // 定义配置
    val config = Map(
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 定义spark环境
    val sparkConf = new SparkConf().setAppName("OfflineRecommender").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 配置需要隐式转换的包
    import spark.implicits._

    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 读取Rating的数据
    val RatingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score)) // 过滤掉时间戳字段
      .cache() // 持久化存在内存中

    // 基于用户和电影评分数据训练ALS模型

    // 用户RDD，需要去重
    val userRDD = RatingRDD.map(_._1).distinct()
    // 电影RDD
    val movieRDD = RatingRDD.map(_._2).distinct()

    // 训练数据,需要转化成spark.mllib里面的Rating类型数据
    val trainData = RatingRDD.map(item => Rating(item._1, item._2, item._3))
    // 训练ALS模型
    val model = ALS.train(trainData, rank, iterations, lambda)
    // 基于用户和电影的隐特征，计算预测评分，得到用户推荐列表
    // 做用户和电影的笛卡尔积
    val userMovies = userRDD.cartesian(movieRDD)
    // 将笛卡尔积传入模型中，得到每个人针对每个电影的预测评分,类型为Rating类型
    val preRatings = model.predict(userMovies)
    // 得到用户电影推荐列表，格式是(uid,(mid,score),(mid,score),(mid,score),...,),按照评分降序
    val userRecs = preRatings
      .filter(_.rating > 0) // 过滤掉预测评分小于0的数据
      // ratingItem为(uid, product_id, score)
      .map(ratingItem => (ratingItem.user, (ratingItem.product, ratingItem.rating)))
      .groupByKey() // 分组聚合，根据用户分组
      .map{
        case (mid, items) =>
          UserRecs(mid, items.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION)
            .map(item => Recommendation(item._1, item._2)))
      }.toDF()
    // 存入mongodb
    storeDataInMongoDB(userRecs, USER_RECS_COLLECTION)

    // 基于电影隐特征 算相关性矩阵
    // 获取电影隐特征,并转化成矩阵
    val movieFeatures = model.productFeatures
      .map{
        case (mid, features) => (mid, new DoubleMatrix(features))
      }
    // 自身与自身做笛卡尔积
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter{
        case (item1, item2) => item1._1 != item2._1
      }
      .map{
        case (item1, item2) => {
          val simScore = this.consinSim(item1._2, item2._2)
          // 包装成(m1,(m2,corr12),(m3,corr13),...)
          (item1._1, (item2._1, simScore))
        }
      }
      // (mid,(mid,simScore))，取simScore就是_._2._2
      .filter(_._2._2 > CORR_MIN_VALUE)
      .groupByKey()
      .map{
        case (mid, items) => MovieRecs(mid, items.toList.sortWith(_._2 > _._2)
        .map(item => Recommendation(item._1, item._2)))
      }.toDF()

    // 写入mongodb
    storeDataInMongoDB(movieRecs, MOVIE_RECS_COLLECTION)
  }

  // 计算余弦相似度
  def consinSim(matrix1: DoubleMatrix, matrix2: DoubleMatrix) : Double = {
    // 分子(点积)除以分母(模长的乘积)
    matrix1.dot(matrix2) / (matrix1.norm2() * matrix2.norm2())
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
