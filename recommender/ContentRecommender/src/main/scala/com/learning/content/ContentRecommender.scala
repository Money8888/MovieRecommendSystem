package com.learning.content

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix


/**
 * 基于内容信息的推荐
 * 需要对电影类别进行TF-IDF特征提取
 */
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

case class Recommendation(mid: Int, score: Double)
// 类别统计数据类
// 基于TF——IDF的特征的相关性列表
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

case class MongoConfig(uri: String, db: String)

object ContentRecommender {
  val MONGODB_MOVIE_COLLECTION = "Movie"
  // 推荐表的名称
  val CONTENT_MOVIE_RECS = "ContentMovieRecs"
  val CORR_MIN_VALUE = 0.6

  def main(args: Array[String]): Unit = {
    // 定义配置
    val config = Map(
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 定义spark环境
    val sparkConf = new SparkConf().setAppName("ContentRecommender").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 配置需要隐式转换的包
    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 读取movie表数据
    val movieTagDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .map(
        // 将类型的中的|改成空格，方便进行TF的转化
        movie => (movie.mid, movie.name, movie.genres.map(c => if(c == '|') ' ' else c))
      )
      .toDF("mid", "name", "genres")
      .cache()

    // TF-IDF提取电影特征向量

    // 创建一个分词器，默认空格分词，用的ml的包，ml用于处理dataset，DataFrame类型的数据结构，mllib用于处理RDD类型的
    val tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words")
    // wordsData格式为[action, adventure, thriller]
    val wordsData = tokenizer.transform(movieTagDF)
    // wordsData.show(truncate = false)
    // 将词语序列转化成对应的词频，采用hash的方式,setNumFeatures为类别数，将超过该值的类用hash碰撞得以规避
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("wordcount").setNumFeatures(50)
    // featureData格式为(50,[19,36,40],[1.0,1.0,1.0])
    val featureData = hashingTF.transform(wordsData)
    // featureData.show(truncate = false)
    // 引入idf模型
    val idf = new IDF().setInputCol("wordcount").setOutputCol("feature")
    // 训练idf模型，得到每个词的逆文本频率
    val idfModel = idf.fit(featureData)
    // 新的tf-idf值,值越大表示区分度越大
    // (50,[19,36,40],[2.0794415416798357,1.7079767945947977,1.8274905761400086]),SparseVector稀疏向量类型
    val tfIdfData = idfModel.transform(featureData)
    tfIdfData.show(truncate = false)
    // 转化成(mid, new DoubleMatrix(features))格式
    val movieFeatures = tfIdfData.map(
      data => (data.getAs[Int]("mid"), data.getAs[SparseVector]("feature").toArray)
    ).rdd
      .map(
        data => (data._1, new DoubleMatrix(data._2))
      )
    // 打印每个rdd，movieFeatures格式是(mid, [0,0,0,3.261684629420357,0,0,0,0.7605551441254693,...]格式
    // movieFeatures.collect().foreach(println)

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
    storeDataInMongoDB(movieRecs, CONTENT_MOVIE_RECS)
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
