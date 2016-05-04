package org.pact518

import org.apache.spark._
import org.apache.spark.mllib.recommendation._
import com.mongodb.casbah.Imports._
/**
  * Created by LeechanXhit on 2016/4/27.
  */
object TestUserRecommendingResult {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("test-user-recommending-result")
    val sc = new SparkContext(sparkConf)
    val testUsers = sc.textFile("hdfs://master:9001/leechanx/netflix/testUsers.txt").map(_.toInt).collect
    val model = MatrixFactorizationModel.load(sc, "hdfs://master:9001/leechanx/netflix/ALSmodel_20160425")
    val recommendingSize = 20
    val mongoClient = MongoClient("master", 27017)
    val collection = mongoClient("RecommendingSystem")("recentRecommeding")
    for (userId <- testUsers) {
      val recommends = model.recommendProducts(userId, recommendingSize).map{rate => (rate.product, rate.rating)}
      val toinsert = MongoDBObject("userId" -> userId, "recommending" -> recommends.map(item => item._1.toString + "," + item._2.toString).mkString("|"), "timedelay" -> 0)
      collection.insert(toinsert)
    }
    sc.stop()
  }
}

