package org.pact518

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import com.redislabs.provider.redis._

import redis.clients.jedis.Jedis

/**
  * Created by LeechanXhit on 2016/4/18.
  */

object similarityStreamingRecommender {
  private val checkpointDir = "popularity-data-checkpoint"
  private val msgConsumerGroup = "user-behavior-topic-message-consumer-group"
  private val minSimilarity = 0.5

  def getUserRecentRatings(sc: SparkContext, sqlctx: SQLContext, K: Int, userId: Int): RDD[(Int, Double)] = {
    //firstly, read recent rating data from redis, if count > K, return it;
    //else, continue to read rating from HDFS Parquet file.
    val todayRatingsRdd = sc.fromRedisHash(userId + "today_ratings")
      .map{ case (movieIdStr, rateAndTimestampStr) =>
        val dataArr: Array[String] = rateAndTimestampStr.trim.split("\\|")
        val rate = dataArr(0).toDouble
        val timestamp = dataArr(1).toLong
        (movieIdStr.toInt, rate, timestamp)
      }
    val count = todayRatingsRdd.count()
    if (count >= K) {
      sc.parallelize(todayRatingsRdd.top(K)(Ordering.by[(Int, Double, Long), Long](_._3)))
        .map{case (movieId, rate, timestamp) => (movieId, rate)}
    } else {
      val stillNeed = K - count
      val rate4user1 = sqlctx.sql("select movieId,rate from ratings where userId = '"
        + userId + "' ORDER BY Timestamp DESC LIMIT " + stillNeed)
      val beforeRatingsRdd = rate4user1.map(eachRow => eachRow.toSeq.map(_.toString)).map{ab => (ab.head.toInt, ab(1).toDouble)}
      todayRatingsRdd.map{ case (movieId, rate, timestamp) => (movieId, rate)}.union(beforeRatingsRdd)
    }
  }

  def getSimilarMovies(sc: SparkContext, movieId: Int): RDD[Int] = {
    sc.fromRedisHash(movieId.toString).map{case (simMovieId, sim) => simMovieId.toInt}
  }

  def getSimilarityBetween2Movies(jedis: Jedis, movieId1: Int, movieId2: Int): Double = {
    jedis.hget(movieId1.toString, movieId2.toString).toDouble
  }

  def createUpdatedRatings_OLD(recentRatings: RDD[(Int, Double)], candidateMovies: RDD[Int]): RDD[(Int, Double)] = {
      val allSimilarityRdd = candidateMovies
        .cartesian(recentRatings)
        .filter{ case(cmovieId, (rmovieId, rate)) => cmovieId != rmovieId}
        .map{ case(cmovieId, (rmovieId, rate)) => (cmovieId, /*getSimilarityBetween2Movies(cmovieId, rmovieId)*/0, rate)}
        .filter(_._2 > minSimilarity) //allSimilarityRdd = [(cmovieId, sim, rate)]
      val numeratorRdd = allSimilarityRdd
          .map{case (cmovieId, sim, rate) => (cmovieId, sim * rate)}.reduceByKey(_ + _) //numeratorRdd = [(cmovieId, sum of sim * rate)]
      val denominatorRdd = allSimilarityRdd
          .map{case (cmovieId, sim, rate) => (cmovieId, sim)}.reduceByKey(_ + _) //denominatorRdd = [(cmovieId, sum of sim)]
      numeratorRdd.join(denominatorRdd).map{ case (cmovieId, (numerator, denominator)) => (cmovieId, numerator / denominator)} //numeratorRdd = [(cmovieId, inferRate)]
    }

  def createUpdatedRatings(recentRatings: RDD[(Int, Double)], candidateMovies: RDD[Int]): RDD[(Int, Double)] = {
    val allSimilarityRdd = candidateMovies
      .cartesian(recentRatings)
      .filter{ case(cmovieId, (rmovieId, rate)) => cmovieId != rmovieId}
      .mapPartitions{ partition =>
        val jedis = new Jedis("192.168.48.166")
        partition.map{ case(cmovieId, (rmovieId, rate)) =>
          (cmovieId, getSimilarityBetween2Movies(jedis, cmovieId, rmovieId), rate)
        }
      }.filter(_._2 > minSimilarity) //allSimilarityRdd = [(cmovieId, sim, rate)]

    val numeratorRdd = allSimilarityRdd
      .map{case (cmovieId, sim, rate) => (cmovieId, sim * rate)}.reduceByKey(_ + _) //numeratorRdd = [(cmovieId, sum of sim * rate)]
    val denominatorRdd = allSimilarityRdd
        .map{case (cmovieId, sim, rate) => (cmovieId, sim)}.reduceByKey(_ + _) //denominatorRdd = [(cmovieId, sum of sim)]
    numeratorRdd.join(denominatorRdd).map{ case (cmovieId, (numerator, denominator)) => (cmovieId, numerator / denominator)} //numeratorRdd = [(cmovieId, inferRate)]
  }

  def mergeToNewRecommends(recentRecommends: RDD[(Int, Double)],
                           updatedRecommends: RDD[(Int, Double)],
                           K: Int): Array[(Int, Double)] = {  //Verified
    recentRecommends.subtractByKey(updatedRecommends)
      .union(updatedRecommends).top(K)(Ordering.by[(Int, Double), Double](_._2))
  }

  def getRecentRecommends(sc: SparkContext, userId: Int): RDD[(Int, Double)] = { //Verified
    sc.fromRedisList(userId.toString).map{ line =>
      val dataArr: Array[String] = line.trim.split("\\|")
      val movieId = dataArr(0).toInt
      val rate = dataArr(1).toDouble
      (movieId, rate)
    }
  }

  def updateRecommends2Redis(sc: SparkContext, newRecommends: Array[(Int, Double)], userId: Int): Unit = {
    val listRdd = sc.parallelize[(Int, Double)](newRecommends)
      .map{ case(movieId, rate) => movieId.toString + "|" + rate.toString}
    sc.toRedisLIST(listRdd, userId.toString)
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("similarityStreamingRecommender")
      .set("redis.host", "localhost").set("redis.port", "6379").set("redis.auth", "")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(3)) //for spark streaming
    val sqlctx = new SQLContext(sc) //for spark SQL

    //set for spark streaming
    ssc.checkpoint(checkpointDir)
    val zkServers = "192.168.48.162:2181"
    val kafkaStream = KafkaUtils.createStream(ssc, zkServers, msgConsumerGroup, Map("user-behavior-topic" -> 3))

    //application for spark sql
    val ratingsDF = sqlctx.read.parquet("") //读取parquetFile of ratings
    ratingsDF.registerTempTable("ratings_table")   //将这个DataFrame注册为一个临时的数据库表，表名为ratings_table

    kafkaStream.foreachRDD(rdd => {
      val newRatesRdd = rdd.map{case (key, msgLine) =>
        val dataArr: Array[String] = msgLine.split("\\|")
        val userID = dataArr(0)
        val productId = dataArr(1)
        val rate = dataArr(2)
        (userID.toInt, productId.toInt, rate.toDouble)
      }
      val allRates = newRatesRdd.collect()
      for ((userId, movieId, rate) <- allRates) {
        val recentRatings = getUserRecentRatings(sc, sqlctx, 10, userId)
        val candidateMovies = getSimilarMovies(sc, movieId)
        val recentRecommends = getRecentRecommends(sc, userId)
        val updatedRecommends = createUpdatedRatings(recentRatings, candidateMovies)
        //updatedRecommends and recentRecommends merge to K recommends.
        val newRecommends = mergeToNewRecommends(recentRecommends, updatedRecommends, 100)
        //and print it.
        for ((rMovieId, inferRate) <- newRecommends) {
          println(userId + "," + rMovieId + "," + inferRate)
        }
        //and store it as next recentRecommends
        updateRecommends2Redis(sc, newRecommends, userId)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
