package org.pact518

import scala.collection.mutable
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import com.mongodb.casbah.Imports.{MongoClient, MongoCollection, MongoDBObject, $set}

/**
  * Created by LeechanX on 2016/5/6.
  */

object streamingRecommender {
  private val msgConsumerGroup = "spark-streaming-topic-message-consumer-group"
  private val minSimilarity = 0.7

  def getUserRecentRatings(collection: MongoCollection, K: Int, userId: Int, movieId: Int, rate: Double): Array[(Int, Double)] = {
    val query = MongoDBObject("userId" -> userId)
    val orderBy = MongoDBObject("timestamp" -> -1)
    val recentRating = collection.find(query).sort(orderBy).limit(K - 1).toArray.map{ item =>
      (item.get("movieId").toString.toInt, item.get("rate").toString.toDouble)
    }.toBuffer
    recentRating += ((movieId, rate))
    recentRating.toArray
  }

  def getSimilarMovies(collectionForProductSimilarity: MongoCollection, collectionForRatingRecords: MongoCollection, movieId: Int, userId: Int, K: Int): Array[Int] = {
    val query = MongoDBObject("Id1" -> movieId)
    val orderBy = MongoDBObject("sim" -> -1)
    val similarMoviesBeforeFilter = collectionForProductSimilarity.find(query).sort(orderBy).limit(K).toArray.map(_.get("Id2").toString.toInt)
    similarMoviesBeforeFilter.filter(haveRatedBefore(collectionForRatingRecords, userId, _) == false)
  }

  def getSimilarityBetween2Movies(collection: MongoCollection, movieId1: Int, movieId2: Int): Double = {
    collection.findOne(MongoDBObject("Id1" -> movieId1, "Id2" -> movieId2)) match {
      case Some(item) => item.get("sim").toString.toDouble
      case None => 0.0
    }
  }

  def createUpdatedRatings(collection: MongoCollection, recentRatings: Array[(Int, Double)], candidateMovies: Array[Int]): mutable.Map[Int, Double] = {
    val allSimilars = mutable.ArrayBuffer[(Int, Double)]()
    for (cmovieId <- candidateMovies; (rmovieId, rate) <- recentRatings) {
      val sim = getSimilarityBetween2Movies(collection, rmovieId, cmovieId)
      if (sim > minSimilarity) {
        allSimilars += ((cmovieId, sim * rate))
      }
    }
    mutable.Map(allSimilars.toArray.groupBy{case (movieId, value) => movieId}
      .map{ case (movieId, simArray) =>
        (movieId, simArray.map(_._2).sum / simArray.length)
      }.toSeq: _*)
  }

  def mergeToNewRecommends(recentRecommends: mutable.Map[Int, Double],
                           updatedRecommends: mutable.Map[Int, Double],
                           K: Int): Array[(Int, Double)] = {  //Verified
    for ((movieId, inferedRate) <- updatedRecommends)
      recentRecommends(movieId) = inferedRate
    recentRecommends.toArray.sortBy{case (movieId, inferedRate) => inferedRate}(Ordering.Double.reverse).slice(0, K)
  }

  def getRecentRecommendsFromMongoDB(collection: MongoCollection, userId: Int, theMovieId: Int): mutable.Map[Int, Double] = { //Verified
  val query = MongoDBObject("userId" -> userId)
    val recommendingArray = collection.findOne(query) match {
      case Some(item) =>
        val recommendingString = item.get("recommending").toString.split("\\|")
        recommendingString.map{
          one =>
            val dataArr = one.split(",")
            val movieId = dataArr.head.toInt
            val predictRate = dataArr(1).toDouble
            (movieId, predictRate)
        }.filter(_._1 != theMovieId)
      case None =>
        Array[(Int, Double)]()
    }
    mutable.Map(recommendingArray: _*)
  }

  def updateRecommends2MongoDB(collection: MongoCollection, newRecommends: Array[(Int, Double)], userId: Int, startTimeMillis: Long): Boolean = {
    val query = MongoDBObject("userId" -> userId)
    val setter = $set("recommending" -> newRecommends.map(item => item._1.toString + "," + item._2.toString).mkString("|"),
      "timedelay" -> (System.currentTimeMillis() - startTimeMillis).toDouble / 1000)
    collection.update(query, setter, upsert = true, multi = false)
    true
  }

  def writeCurrentRating2MongoDB(collection: MongoCollection, userId: Int, movieId: Int, rate: Double, timestamp: Long): Unit = {
    val toInsert = MongoDBObject("userId" -> userId, "movieId" -> movieId, "rate" -> rate, "timestamp" -> timestamp)
    collection.insert(toInsert)
  }

  def haveRatedBefore(collection: MongoCollection, userId: Int, movieId: Int): Boolean = {
    collection.findOne(MongoDBObject("userId" -> userId, "movieId" -> movieId)) match {
      case Some(item) => true
      case None => false
    }
  }

  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Usage:")
      println("xxxx.jar MasterHost Topic-Name")
      System.exit(1)
    }
    val masterHost = args(0)
    val topicName = args(1)

    val sparkConf = new SparkConf().setAppName("similarityStreamingRecommender")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(3)) //for spark streaming

    val zkServers = masterHost + ":2181"
    val kafkaStream = KafkaUtils.createStream(ssc, zkServers, msgConsumerGroup, Map(topicName -> 3))
    val K: Int = 20

    object DistributedMongoDB extends Serializable {
      @transient private val mongoClient = MongoClient("localhost", 27017)
      def getCollection(collectionName: String): MongoCollection = {
        mongoClient("RecommendingSystem")(collectionName)
      }
    }

    object SingleMongoDB extends Serializable {
      @transient private val mongoClient = MongoClient(masterHost, 27017)
      def getCollection(collectionName: String): MongoCollection = {
        mongoClient("RecommendingSystem")(collectionName)
      }
    }

    val dataDStream = kafkaStream.map{ case (key, msgLine) =>
      val dataArr: Array[String] = msgLine.split("\\|")
      val userId = dataArr(0).toInt
      val movieId = dataArr(1).toInt
      val rate = dataArr(2).toDouble
      val startTimeMillis = dataArr(3).toLong
      (userId, movieId, rate, startTimeMillis)
    }

    dataDStream.foreachRDD(rdd => {
      rdd.map{ case (userId, movieId, rate, startTimeMillis) =>
        val recentRatings = getUserRecentRatings(DistributedMongoDB.getCollection("ratingRecords"), K, userId, movieId, rate)
        val candidateMovies = getSimilarMovies(DistributedMongoDB.getCollection("productSimilarity"), DistributedMongoDB.getCollection("ratingRecords"), movieId, userId, K)
        val recentRecommends = getRecentRecommendsFromMongoDB(SingleMongoDB.getCollection("recentRecommending"), userId, movieId)
        val updatedRecommends = createUpdatedRatings(DistributedMongoDB.getCollection("productSimilarity"), recentRatings, candidateMovies)
        val newRecommends = mergeToNewRecommends(recentRecommends, updatedRecommends, K)
        updateRecommends2MongoDB(SingleMongoDB.getCollection("recentRecommending"), newRecommends, userId, startTimeMillis)
        //writeCurrentRating2MongoDB(SingleMongoDB.getCollection("ratingRecords"), userId, movieId, rate, startTimeMillis)
        //DESCRIPTION: no need for testing, so comment it right now.
      }.count()
    })

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val movieIdCount = dataDStream.map{case (userId, movieId, rate, startTimeMillis) => (movieId, 1)}
    val stateDstream = movieIdCount.updateStateByKey[Int](updateFunc).cache()
    stateDstream.foreachRDD{rdd =>
      val hotMovies = rdd.top(5)(Ordering.by[(Int, Int), Int](_._2))
      for ((movieId, counter) <- hotMovies) {
        println(movieId + ":" + counter)
      }
    }

    ssc.checkpoint("hdfs://master:9001/leechanx/netflix/checkpoint_dir")
    ssc.start()
    ssc.awaitTermination()
  }
}

