package com.wmm.spark.rdd.operator.transform.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Map算子主要目的将数据源中的数据进行转换和改变。但是不会减少或增多数据。
 * Map算子是分区内一个数据一个数据的执行，类似于串行操作。
 */
object MapDemo extends App {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  //从内存中创建RDD
  val rdd = sparkContext.parallelize(List(1, 2, 3, 4), 2)
  rdd.map(_ * 2).foreach(println)
}
