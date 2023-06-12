package com.wmm.spark.rdd.operator.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.time.Year

/**
 * 在类型为(K,V)和(K,W)的 RDD 上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的 RDD
 */
object CoGroupDemo extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  val rdd1: RDD[(String, Int)] = sparkContext.makeRDD(List(("a", 88), ("a", 88), ("b", 95), ("c", 91), ("d", 93)), 2)
  private val year = Year.now()
  val rdd2: RDD[(String, Year)] = sparkContext.makeRDD(List(("a", year.plusYears(1)), ("a", year.plusYears(1)), ("b", year.plusYears(2)), ("c", year)), 2)
  val leftOuterJoinedRDD: RDD[(String, (Iterable[Int], Iterable[Year]))] = rdd1.cogroup(rdd2)
  leftOuterJoinedRDD.foreach(println)
}
