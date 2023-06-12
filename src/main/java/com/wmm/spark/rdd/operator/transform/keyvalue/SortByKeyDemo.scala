package com.wmm.spark.rdd.operator.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 在一个(K,V)的 RDD 上调用，K 必须实现 Ordered 接口(特质)，返回一个按照 key 进行排序的RDD
 */
object SortByKeyDemo extends App {
  //求每个key的平均值
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  val rdd: RDD[(String, Int)] = sparkContext.makeRDD(List(("a", 88), ("b", 95), ("c", 91), ("d", 93)), 2)
  val sortedRDD: RDD[(String, Int)] = rdd.sortByKey()
  sortedRDD.foreach(println)
}
