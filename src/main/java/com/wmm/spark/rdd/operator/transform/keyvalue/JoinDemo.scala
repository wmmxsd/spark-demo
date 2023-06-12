package com.wmm.spark.rdd.operator.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.time.Year

/**
 * 在类型为(K,V)和(K,W)的 RDD 上调用，返回一个相同 key 对应的所有元素连接在一起的(K,(V,W))的 RDD
 * 类似于sql的inner join
 */
object JoinDemo extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  val rdd1: RDD[(String, Int)] = sparkContext.makeRDD(List(("a", 88), ("a", 88), ("b", 95), ("c", 91), ("d", 93)), 2)
  val rdd2: RDD[(String, Year)] = sparkContext.makeRDD(List(("a", Year.now().plusYears(1)), ("a", Year.now().plusYears(1)), ("b", Year.now().plusYears(2)), ("c", Year.now())), 2)
  val joinedRDD: RDD[(String, (Int, Year))] = rdd1.join(rdd2)
  joinedRDD.foreach(println)
}
