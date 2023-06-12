package com.wmm.spark.rdd.operator.transform.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 以一个 RDD 元素为主，去除两个 RDD 中重复元素，将其他元素保留下来。求差集
 */
object SubtractDemo extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  //从内存中创建RDD
  val rdd1: RDD[Any] = sparkContext.parallelize(List(1, 2, 3, 4, 2, 2, 2, "j"), 2)
  val rdd2: RDD[Any] = sparkContext.parallelize(List(1, 2, 3, "j", '2'), 2)
  val rdd = rdd1.subtract(rdd2)
  //4
  rdd.foreach(println)
}
