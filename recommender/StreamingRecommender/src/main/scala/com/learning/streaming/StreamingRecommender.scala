package com.learning.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

// 连接对象
object ConnHelper extends Serializable{
  // 懒加载 用时加载，初始化会延迟，不用重复加载
  lazy val jedis = new Jedis("localhost")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))
}

case class Recommendation(mid: Int, score: Double)
// 对每个用户的电影列表推荐类
case class UserRecs(uid: Int, recs: Seq[Recommendation])
// 用于读入的相关性列表
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

case class MongoConfig(uri: String, db: String)

object StreamingRecommender {
  // 推荐给用户的数量
  val MAX_USER_RATINGS_NUM = 20
  // 备选的推荐列表，从相关性矩阵中拿取
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    // 定义spark环境
    val sparkConf = new SparkConf().setAppName("StreamingRecommender").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 获取sc
    val sc = spark.sparkContext
    // 获取sparkStreamingContext, 第二个参数为batch duration,批次持续时间，微批处理
    val ssc = new StreamingContext(sc, Seconds(2))

    // 配置需要隐式转换的包
    import spark.implicits._

    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 导入电影相似度矩阵数据
    val simMoviesMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("org.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      // 转成{mid1:{mid2: corr}}类型
      .map { movieRecs =>
        (movieRecs.mid, movieRecs.recs.map( x => (x.mid, x.score)).toMap)
      }.collectAsMap()

    // 因为数据量可能很大，作为广播变量广播出去，分发给每个executor
    val simMoviesMatrixBroadCast = sc.broadcast(simMoviesMatrix)

    // 定义kafka配置项
    val kafkaParam = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      // 偏移量策略 为最近的，保留最近的
      "auto.offset.reset" -> "latest"
    )

    // 创建kafka流，泛型为(String, String)，和kafkaParam的key，value类型一致
    val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc,
      // 一致性的方式分配分区所有 executor 上（主要是为了分布均匀）
      LocationStrategies.PreferConsistent,
      // 消费者策略
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParam))

    // kafka消息格式为UID|MID|SCORE|TIMESTAMP
    val ratingStream = kafkaStream.map{
      message => {
        val attr = message.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
      }
    }

    // 对消息进行业务流处理
    ratingStream.foreachRDD{
          // 拿到的是一个时间窗口的一组rdd，而ratingStream可以是多个时间窗口
      rdds => rdds.foreach{
        case (uid, mid, score, timestamp) =>
          println("data coming >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
          // 1、从redis加载用户最近评分过的数据保存为 Array[(uid, score)]
          val userRecentlyRatings = getUserRecentlyRatings( MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis )
          // 2、从相似度矩阵中获取备选电影列表,并过滤掉已看过的电影，保存为Array[mid]
          val candidateMoviesIds = getTopSimMovies( MAX_SIM_MOVIES_NUM, mid, uid, simMoviesMatrixBroadCast.value )
          // 3、按照公式计算备选列表中的每个电影的优先级值，保存Array[(mid,score)]
          val streamRecs = computeMovieScores( candidateMoviesIds, userRecentlyRatings, simMoviesMatrixBroadCast.value )
          // 4、存入mongodb
          saveDataToMongoDB(uid, streamRecs)
      }
    }
    // 开启流处理
    ssc.start()
    println("start streaming ")
    // 等待终止
    ssc.awaitTermination()
  }

  // 将返回的java类转变成scala能操作的类
  import scala.collection.JavaConversions._
  def getUserRecentlyRatings(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
    // 从redis里面读取数据，redis的key为"uid:1","uid:2"等，value是"123:3.4"
    // lrange相当于出栈操作，序号靠前的元素是最新的
    jedis.lrange("uid:" + uid, 0, num - 1)
      .map{
        // 得到的是List，且每个元素value是以:分割
        item => {
          val split = item.split("\\:")
          (split(0).trim.toInt, split(1).trim.toDouble)
        }
      }.toArray
  }

  /**
   * 获取与当前mid电影最相似的电影列表
   * @param num 个数
   * @param mid 当前电影id
   * @param uid 当前用户id，做过滤用
   * @param simMovies 相似度矩阵，广播变量
   * @param mongoConfig  mongo的配置
   * @return
   */
  // scala.collection.immutable.Map 不可变的map
  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                     (implicit mongoConfig: MongoConfig): Array[Int] = {
    // 从相似度矩阵中拿出所有相似的电影
    val allSimMovies = simMovies(mid).toArray

    // 从mongodb里面查询用户看过的电影
    val ratingExists = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("uid" -> uid))
      .toArray
      // 查出来的是一个json的map形式,只需要mid的数据
      .map{
        item => item.get("mid").toString.toInt
      }

    // 过滤
    allSimMovies.filter( movies => ratingExists.contains(movies._1) )
      .take(num)
      .map( movie => movie._1 )
  }

  def computeMovieScores(candidateMoviesIds: Array[Int], userRecentlyRatings: Array[(Int, Double)], simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Array[(Int, Double)] = {
    // 代入公式 E_{uq} = \frac{\sum_{r \in RK} sim(q,r) * Rr}{sim\_sum} + lgmax\{incount, 1\} - lgmax\{recount, 1\}
    // 分别计算三项，定义三个计数器
    // 保存每一个备选的电影的基础得分
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    // 定义两个由于好评差评增强减弱项的hashmap，对应的类型是(候选的mid, 个数)
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    // 循环每个备选电影和最近评分过的电影
    for(candidateMovieId <- candidateMoviesIds; userRecentlyRating <- userRecentlyRatings){
      val simScore = getSimScore(candidateMovieId, userRecentlyRating._1, simMovies)
      if(simScore > 0.7){
        // += 类似array的函数，进行了运算符的重载
        scores +=((candidateMovieId, simScore * userRecentlyRating._2))
        // 更新增强减弱因子
        if (userRecentlyRating._2 > 3){
          // 大于3为好评
          increMap(candidateMovieId) = increMap.getOrDefault(candidateMovieId, 0) + 1
        }else{
          decreMap(candidateMovieId) = decreMap.getOrDefault(candidateMovieId, 0) + 1
        }
      }
    }

    // 根据备选的mid做分组聚合公式计算
    scores.groupBy(_._1).map{
          // groupby之后的数据结构是Map(mid -> ArrayBuffer[(mid, score)])
      case (mid, scoreList) => {
        (mid, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(mid, 1)) - log(decreMap.getOrDefault(mid, 1)))
      }
    }.toArray.sortWith(_._2 > _._2)

  }

  def getSimScore(mid1: Int, mid2: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Double = {
    // 需要做匹配，没找到的数据给默认值
    simMovies.get(mid1) match {
      case None => 0.0
      case Some(sims) => sims.get(mid2) match {
        case None => 0.0
        case Some(mid) => mid
      }
    }
  }

  def log(data: Int): Double = {
    val N = 10
    // 换底公式
    Math.log(data) / Math.log(N)
  }

  def saveDataToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    // 如果数据存在则删除
    streamRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))
    // 插入数据
    streamRecsCollection.insert(MongoDBObject("uid" -> uid, "recs" -> streamRecs.map(items => MongoDBObject("mid" -> items._1, "score" -> items._2))))
  }

}
