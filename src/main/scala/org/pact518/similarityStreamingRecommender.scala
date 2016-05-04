package org.pact518

import util.control.Breaks._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import com.mongodb.casbah.Imports._

/**
  * Created by LeechanXhit on 2016/4/18.
  */

object similarityStreamingRecommender {
  private val checkpointDir = "popularity-data-checkpoint"
  private val msgConsumerGroup = "user-behavior-topic-message-consumer-group"
  private val minSimilarity = 0.6

  def getUserRecentRatings(mongoClient: MongoClient, sc: SparkContext, mongoMaster: String, K: Int, userId: Int): RDD[(Int, Double)] = {
    //read recent rating data from MongoDB, return it;
    val collection = mongoClient("RecommendingSystem")("ratingRecords")
    val query = MongoDBObject("userId" -> userId)
    val orderBy = MongoDBObject("timestamp" -> -1)
    val recentKRatings = collection.find(query).sort(orderBy).limit(K).toArray.map{ item =>
      (item.get("movieId").toString.toInt, item.get("rate").toString.toDouble)
    }
    sc.parallelize(recentKRatings)
  }

  def getSimilarMovies(mongoClient: MongoClient, sc: SparkContext, movieId: Int, userId: Int): RDD[Int] = {
    //sc.fromRedisHash(movieId.toString).map{case (simMovieId, sim) => simMovieId.toInt}
    val collection = mongoClient("RecommendingSystem")("productSimilarity")
    val query = MongoDBObject("Id1" -> movieId) ++ ("sim" $gt  0.6)
    val similarMoviesBeforeFilter = collection.find(query).toArray.map(_.get("Id2").toString.toInt)
    //filter the movie that user has rated...
    val similarMovies = similarMoviesBeforeFilter.filter(haveRatedBefore(mongoClient, userId, _))
    sc.parallelize(similarMovies)
  }

  def getSimilarityBetween2Movies(collection: MongoCollection, movieId1: Int, movieId2: Int): Double = {
    val queryResult = collection.findOne(MongoDBObject("Id1" -> movieId1, "Id2" -> movieId2))
    queryResult match {
      case Some(item) => item.get("sim").toString.toDouble
      case None => 0.0
    }
  }

  def createUpdatedRatings(mongoMaster: String, recentRatings: RDD[(Int, Double)], candidateMovies: RDD[Int]): RDD[(Int, Double)] = {
    val allSimilarityRdd = candidateMovies
      .cartesian(recentRatings)
      .filter{ case(cmovieId, (rmovieId, rate)) => cmovieId != rmovieId}
      .mapPartitions{ partition =>
        val mongoClient = MongoClient(mongoMaster, 27017)
        val collection = mongoClient("RecommendingSystem")("productSimilarity")
        partition.map{ case(cmovieId, (rmovieId, rate)) =>
          (cmovieId, getSimilarityBetween2Movies(collection, cmovieId, rmovieId), rate)
        }
      }.filter(_._2 > minSimilarity) //allSimilarityRdd = [(cmovieId, sim, rate)]

    val numeratorRdd = allSimilarityRdd
      .map{case (cmovieId, sim, rate) => (cmovieId, sim * rate)}.reduceByKey(_ + _) //numeratorRdd = [(cmovieId, sum of sim * rate)]
    val denominatorRdd = allSimilarityRdd
        .map{case (cmovieId, sim, rate) => (cmovieId, 1)}.reduceByKey(_ + _) //denominatorRdd = [(cmovieId, counter)]
    numeratorRdd.join(denominatorRdd).map{ case (cmovieId, (totalInferRates, counter)) => (cmovieId, totalInferRates / counter)} //numeratorRdd = [(cmovieId, inferRate)]
  }

  def mergeToNewRecommends(recentRecommends: RDD[(Int, Double)],
                           updatedRecommends: RDD[(Int, Double)],
                           K: Int): Array[(Int, Double)] = {  //Verified
    recentRecommends.subtractByKey(updatedRecommends)
      .union(updatedRecommends).top(K)(Ordering.by[(Int, Double), Double](_._2))
  }

  def getRecentRecommendsFromMongoDB(sc: SparkContext, mongoClient: MongoClient, userId: Int, theMovieId: Int): RDD[(Int, Double)] = { //Verified
  val collection = mongoClient("RecommendingSystem")("recentRecommeding")
    val query = MongoDBObject("userId" -> userId)
    val recommendingString = collection.findOne(query).get.get("recommending").toString.split("\\|")
    sc.parallelize(recommendingString.map{
      one =>
        val dataArr = one.split(",")
        val movieId = dataArr.head.toInt
        val predictRate = dataArr(1).toDouble
        (movieId, predictRate)
    }).filter(_._1 != theMovieId)
  }

  def updateRecommends2MongoDB(mongoClient: MongoClient, newRecommends: Array[(Int, Double)], userId: Int, startTimeMillis: Long): Unit = {
    val collection = mongoClient("RecommendingSystem")("recentRecommeding")
    val query = MongoDBObject("userId" -> userId)
    val setter = $set("recommending" -> newRecommends.map(item => item._1.toString + "," + item._2.toString).mkString("|"), "timedelay" -> (System.currentTimeMillis() - startTimeMillis))
    collection.update(query, setter)
  }

  def haveRatedBefore(mongoClient: MongoClient, userId: Int, movieId: Int): Boolean = {
    val collection = mongoClient("RecommendingSystem")("ratingRecords")
    collection.findOne(MongoDBObject("userId" -> userId, "movieId" -> movieId)) match {
      case Some(item) => true
      case None => false
    }
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("similarityStreamingRecommender")
      //.set("redis.host", "localhost").set("redis.port", "6379").set("redis.auth", "")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(3)) //for spark streaming

    //set for spark streaming
    ssc.checkpoint(checkpointDir)
    val zkServers = "192.168.48.162:2181"
    val kafkaStream = KafkaUtils.createStream(ssc, zkServers, msgConsumerGroup, Map("user-behavior-topic" -> 3))

    val mongoMaster = args(0)

    kafkaStream.foreachRDD(rdd => {
      val newRatesRdd = rdd.map{case (key, msgLine) =>
        val dataArr: Array[String] = msgLine.split("\\|")
        val userID = dataArr(0)
        val productId = dataArr(1)
        val rate = dataArr(2)
        (userID.toInt, productId.toInt, rate.toDouble, System.currentTimeMillis())
      }
      val allRates = newRatesRdd.collect()
      val mongoClient = MongoClient(mongoMaster, 27017)
      for ((userId, movieId, rate, startTimeMillis) <- allRates) {
        breakable {
          if (haveRatedBefore(mongoClient, userId, movieId)) break()
          val recentRatings = getUserRecentRatings(mongoClient, sc, mongoMaster, 10, userId)
          val candidateMovies = getSimilarMovies(mongoClient, sc, movieId, userId)
          val recentRecommends = getRecentRecommendsFromMongoDB(sc, mongoClient, userId, movieId)
          val updatedRecommends = createUpdatedRatings(mongoMaster, recentRatings, candidateMovies)
          //updatedRecommends and recentRecommends merge to K recommends.
          val newRecommends = mergeToNewRecommends(recentRecommends, updatedRecommends, 100)
          //and print it.
          for ((rMovieId, inferRate) <- newRecommends) {
            println(userId + "," + rMovieId + "," + inferRate)
          }
          //and store it as next recentRecommends
          updateRecommends2MongoDB(mongoClient, newRecommends, userId, startTimeMillis)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
