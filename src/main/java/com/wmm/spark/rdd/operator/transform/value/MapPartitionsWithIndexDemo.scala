package com.wmm.spark.rdd.operator.transform.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 带分区索引的MapPartitions
 */
object MapPartitionsWithIndexDemo extends App {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  //从内存中创建RDD
  val rdd = sparkContext.parallelize(List(1, 2, 3, 4), 2)
  rdd.mapPartitionsWithIndex((indexedSeq, ints: Iterator[Int]) => ints.map((indexedSeq, _))).foreach(println)
  rdd.mapPartitionsWithIndex((_, ints) => ints.map(i => i)).foreach(println)
}
