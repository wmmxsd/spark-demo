package com.wmm.spark.rdd.operator.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.time.Year

/**
 * 类似于 SQL 语句的左外连接
 */
object LeftOuterJoinDemo extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  val rdd1: RDD[(String, Int)] = sparkContext.makeRDD(List(("a", 88), ("b", 95), ("c", 91), ("d", 93)), 2)
  val rdd2: RDD[(String, Year)] = sparkContext.makeRDD(List(("a", Year.now().plusYears(1)), ("b", Year.now().plusYears(2)), ("c", Year.now())), 2)
  val leftOuterJoinedRDD: RDD[(String, (Int, Option[Year]))] = rdd1.leftOuterJoin(rdd2)
  leftOuterJoinedRDD.foreach(println)
}
