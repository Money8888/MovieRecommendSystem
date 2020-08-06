package com.learning.offline

import breeze.numerics.sqrt
import com.learning.offline.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {

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
    // 同一个包下声明的类共用
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 读取Rating的数据
    val RatingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => Rating(rating.uid, rating.mid, rating.score)) // 过滤掉时间戳字段,并保存成Rating格式
      .cache() // 持久化存在内存中

    // 划分数据集
    val split = RatingRDD.randomSplit(Array(0.8, 0.2), seed = 1)
    val trainData = split(0)
    val testData = split(1)

    // 训练测试模型，找出最佳超参数组合
    // 无返回值，直接打印在控制台
    adjustModelParam(trainData, testData)

    spark.close()

  }

  def adjustModelParam(trainData: RDD[Rating], testData: RDD[Rating]): Unit = {
    // 循环遍历各个超参数组合
    val result = for(rank <- Array(30, 50, 100, 200); lambda <- Array(0.001, 0.01, 0.05, 0.1))
      // 需要保存每次循环的中间结果，用yield
      yield {
        // 训练模型,迭代次数不宜过多，否则会栈溢出
        val model = ALS.train(trainData, rank, 5, lambda)
        // 求RMSE
        val rmse = getRMSE(model, testData)
        // 返回参数和RMSE
        (rank, lambda, rmse)
      }
    // 打印出来最小的RMSE组合
    println(result.minBy(_._3))
  }

  // 计算RMSE
  def getRMSE(model: MatrixFactorizationModel, testData: RDD[Rating]):Double = {
    // 带入模型的数据只需要uid,mid，不需要评分值，所以需去掉
    val userProducts = testData.map(item => (item.user, item.product))
    // 代入模型，并转化成((uid, mid), rating)格式,以(uid, mid)为唯一键进行内连接
    val preRatings = model.predict(userProducts).map(item => ((item.user, item.product), item.rating))
    // 原始值,并转化成((uid, mid), rating)格式,以(uid, mid)为唯一键进行内连接
    val observes = testData.map(item => ((item.user, item.product), item.rating))


    // 计算RMSE
    sqrt(
      observes.join(preRatings)
        .map{
          case ((uid, mid), (real, pre)) =>
            val error = real - pre
            // 返回误差的平方
            error * error
        }.mean()
    )

  }

}
