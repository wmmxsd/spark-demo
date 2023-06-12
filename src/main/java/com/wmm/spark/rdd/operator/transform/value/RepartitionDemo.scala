package com.wmm.spark.rdd.operator.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 重新分区
 * 可以增加或降低此 RDD 中的并行级别。
 * 如果要减少此 RDD 中的分区数，请考虑使用coalesce。
 *
 * [[com.wmm.spark.rdd.operator.transform.CoalesceDemo]]
 *
 * repartition方法的底层就是调用的shuffle为true的coalesce方法，
 */
object RepartitionDemo extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  //从内存中创建RDD
  val rdd: RDD[Int] = sparkContext.parallelize(List(1, 2, 3, 4, 2, 2, 2), 6)
  println(rdd.getNumPartitions)
  val rdd1: RDD[Int] = rdd.repartition(2)
  println(rdd1.getNumPartitions)
}
