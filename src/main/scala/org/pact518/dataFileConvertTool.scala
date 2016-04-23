package org.pact518

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import redis.clients.jedis.Jedis

/**
  * Created by LeechanX on 2016/4/18.
  * 两个工具，分别转化评分记录文件为Parquet格式、把相似度放到Redis中
  */

object dataFileConvertTool {
  def convertRatingFile2ParquetFile(sc: SparkContext, sqlctx: SQLContext, filePath: String): Unit = {
    val ratingsData = sc.textFile(filePath)
    val schemaString = "userId,movieId,rate,timestamp"
    val schemaArray = schemaString.split(",")
    val schema = StructType(schemaArray.map{fieldName => StructField(fieldName, StringType, nullable = true)})
    val rowRDD: RDD[Row] = ratingsData.map(_.split(","))
      .map(eachRow => Row(eachRow(0), eachRow(1), eachRow(2), eachRow(3)))
    val ratingsDF = sqlctx.createDataFrame(rowRDD, schema) //列名对应，分别为 userId, movieId, rate,timestamp
    ratingsDF.registerTempTable("ratings_table")   //将这个DataFrame注册为一个临时的数据库表，表名为ratings_table
    ratingsDF.write.parquet(filePath + "_parquet") //存储到Parquet方式
    println("save as parquet file OK, path " + filePath + "_parquet")
  }

  def writeSimilarityFile2Redis(sc: SparkContext, filePath: String): Long = {
    val similarityData = sc.objectFile[(Int, Int, Double)](filePath)
    similarityData.mapPartitions{ partition =>
      val jedis = new Jedis("192.168.48.166")
      partition.map{ case (movieId1, movieId2, simi) =>
        jedis.hset(movieId1.toString, movieId2.toString, simi.toString)
      }
    }.count()
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dataFileConvertTool")
    val sc = new SparkContext(sparkConf)
    val sqlctx = new SQLContext(sc)
    convertRatingFile2ParquetFile(sc, sqlctx, "hdfs://master:9001/leechanx/netflix/trainingData.txt")
    writeSimilarityFile2Redis(sc, "")
    sc.stop()
  }
}
