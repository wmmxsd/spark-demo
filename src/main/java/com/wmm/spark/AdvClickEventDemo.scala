package com.wmm.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从click-events.log文件中统计出每一个省份每个广告被点击数量排行的 Top3
 */
object AdvClickEventDemo extends App {
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("advClickEvent")
  val sparkContext: SparkContext = new SparkContext(conf)
  val logTextRDD: RDD[String] = sparkContext.textFile(this.getClass.getResource("/click-events.log").getPath)
  
  val advClickEventRDD: RDD[((String, String), Int)] = logTextRDD.map(log => {
    val logArr: Array[String] = log.split(" ")
    ((logArr(1), logArr(4)), 1)
  })

  val reduceByKeyRDD: RDD[((String, String), Int)] = advClickEventRDD.reduceByKey(_ + _)
  val mapRDD: RDD[(String, (String, Int))] = reduceByKeyRDD.map((tuple: ((String, String), Int)) => (tuple._1._1, (tuple._1._2, tuple._2)))
  val groupByKeyRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()
  val result: Array[(String, List[(String, Int)])] = groupByKeyRDD.mapValues((tuples: Iterable[(String, Int)]) => tuples.toList.sortBy((tuple: (String, Int)) => tuple._2)(Ordering.Int.reverse)).take(3)
  val result1: Array[(String, List[(String, Int)])] = groupByKeyRDD.map((tuple: (String, Iterable[(String, Int)])) => (tuple._1, tuple._2.toList.sortBy((tuple: (String, Int)) => tuple._2)(Ordering.Int.reverse))).take(3)
  result.foreach(println)
  result1.foreach(println)
}