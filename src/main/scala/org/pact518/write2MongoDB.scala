package org.pact518

import org.apache.spark._
import org.apache.spark.mllib.recommendation._
import com.mongodb.casbah.Imports._
/**
  * Created by LeechanXhit on 2016/4/24.
  */
object write2MongoDB {

  def writeSimilarityFile2MongoDB(sc: SparkContext, filePath: String, mongoMaster: String): Unit = {
    val similarityRdd = sc.textFile(filePath).cache()
    val store2MonogoDB = similarityRdd.mapPartitions{
      partition => {
        val mongoClient = MongoClient(mongoMaster, 27017)
        val collection = mongoClient("RecommendingSystem")("productSimilarity")
        partition.map{
          str =>
            val dataArr = str.split(",")
            val movieId1 = dataArr.head.toInt
            val movieId2 = dataArr(1).toInt
            val sim = dataArr(2).toDouble
            val toInsert = MongoDBObject("Id1" -> movieId1, "Id2" -> movieId2, "sim" -> sim)
            collection.insert(toInsert)
            true
        }
      }
    }
    store2MonogoDB.count()
    similarityRdd.unpersist()
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("USAGE:")
      println("spark-submit ... xxx.jar Date_String MongoDB_Master_IP_OR_HOST")
      println("For example: spark-submit ... xxx.jar 20160424 master")
      sys.exit()
    }
    val date = args(0)
    val mongoMaster = args(1)
    val dataPath = "hdfs://master:9001/leechanx/netflix/"
    val sparkConf = new SparkConf().setAppName("dataFileConvertTool")
      .set("spark.executor.memory", "3g")
      .set("spark.driver.memory", "6g")
    val sc = new SparkContext(sparkConf)

    writeSimilarityFile2MongoDB(sc, dataPath + "allSimilarity_" + date, mongoMaster)

    sc.stop()
  }
}

