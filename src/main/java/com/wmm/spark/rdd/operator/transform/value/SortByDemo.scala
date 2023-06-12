package com.wmm.spark.rdd.operator.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 根据特定逻辑进行排序
 */
object SortByDemo extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  //从内存中创建RDD
  val rdd: RDD[Int] = sparkContext.parallelize(List(1, 2, 3, 4, 2, 2, 2), 1)
  rdd.foreach(println)
  rdd.sortBy(num => num, ascending = false).foreach(println)
  rdd.sortBy(num => num, ascending = true).foreach(println)
}
