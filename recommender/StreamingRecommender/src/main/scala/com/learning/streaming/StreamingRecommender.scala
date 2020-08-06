package com.learning.streaming

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import redis.clients.jedis.Jedis

// 连接对象
object ConnHelper extends Serializable{
  // 懒加载 用时加载
  lazy val jedis = new Jedis("localhost")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))
}

object StreamingRecommender {

}
