package com.wmm.spark.rdd.operator.transform.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * flatMap demo
 */
object FlatMapDemo extends App {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  //从内存中创建RDD
  val rdd = sparkContext.parallelize(List(List(1, 2), List(3, 4)))
  //1
  //2
  //3
  //4
  rdd.flatMap(list => list).foreach(println)
  //List(3, 4)
  //List(1, 2)
  rdd.map(list => list).foreach(println)
}
