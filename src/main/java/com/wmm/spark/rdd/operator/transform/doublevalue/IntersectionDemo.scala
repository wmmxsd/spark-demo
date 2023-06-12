package com.wmm.spark.rdd.operator.transform.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 对源 RDD 和参数 RDD 求交集后返回一个新的 RDD
 */
object IntersectionDemo extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  //从内存中创建RDD
  val rdd1: RDD[Any] = sparkContext.parallelize(List(1, 2, 3, 4, 2, 2, 2, "j"), 2)
  val rdd2: RDD[Any] = sparkContext.parallelize(List(1, 2, 3, "j", '2'), 2)
  val rdd = rdd1.intersection(rdd2)
  rdd.foreach(println)
}
