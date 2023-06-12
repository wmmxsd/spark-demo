package com.wmm.spark.rdd.operator.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 合并分区
 * 根据数据量缩减分区，用于大数据集过滤后，提高小数据集的执行效率
 * 也可以增加分区数，shuffle的值必须为true，为true时会进行shuffle操作，不带有分区捆绑进行重新分区，分区更加均匀（避免数据倾斜）
 *
 */
object CoalesceDemo extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  //从内存中创建RDD
  val rdd: RDD[Int] = sparkContext.parallelize(List(1, 2, 3, 4, 2, 2, 2), 6)
  println(rdd.getNumPartitions)
  //缩减分区
  val rdd1: RDD[Int] = rdd.coalesce(2)
  println(rdd1.getNumPartitions)
  //增加分区
  val rdd2: RDD[Int] = rdd.coalesce(8, shuffle = true)
  println(rdd2.getNumPartitions)
}
