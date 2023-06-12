package com.wmm.spark.rdd.operator.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * MapPartitions需要传递一个迭代器，返回一个迭代器，没有要求的元素的个数保持不变，所以可以增加或减少数据。
 * 是以分区为单位进行批处理操作。
 */
object MapPartitionsDemo extends App {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  //从内存中创建RDD
  val rdd: RDD[Int] = sparkContext.parallelize(List(1, 2, 3, 4), 2)
  rdd.mapPartitions(_.filter(_ % 2 == 0)).foreach(println)
  rdd.mapPartitions((ints: Iterator[Int]) => ints.filter((i: Int) => i % 2 == 0)).foreach(println)
}
