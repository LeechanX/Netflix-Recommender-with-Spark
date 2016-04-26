package org.pact518

import org.apache.spark._
import com.mongodb.casbah.Imports._
/**
  * Created by LeechanXhit on 2016/4/25.
  */
object tempTool {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("USAGE:")
      println("spark-submit ... xxx.jar MongoDB_Master_IP_OR_HOST")
      println("For example: spark-submit ... xxx.jar master")
      sys.exit()
    }
    val mongoMaster = args(0)
    val sparkConf = new SparkConf().setAppName("dataFileConvertTool")
      .set("spark.executor.memory", "3g")
      .set("spark.driver.memory", "6g")
    val sc = new SparkContext(sparkConf)

    val ratingRdd = sc.textFile("hdfs://master:9001/leechanx/netflix/trainingData.txt")
      .mapPartitions{ partition =>
        val mongoClient = MongoClient(mongoMaster, 27017)
        val collection = mongoClient("RecommendingSystem")("ratingRecords")
        partition.map{ str =>
          val dataArr = str.split(",")
          val movieId = dataArr(0).toInt
          val userId = dataArr(1).toInt
          val rate = dataArr(2).toDouble
          val timestamp = dataArr(3).toLong
          val toInsert = MongoDBObject("userId" -> userId, "movieId" -> movieId, "rate" -> rate, "timestamp" -> timestamp)
          collection.insert(toInsert)
          true
        }
      }
    ratingRdd.count()
    sc.stop()
  }
}

