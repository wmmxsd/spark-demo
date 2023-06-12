package com.wmm.spark.rdd.operator.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}

object PartitionByDemo extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  //从内存中创建RDD
  val rdd: RDD[(Int, Char)] = sparkContext.makeRDD(List((1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f')))
  println(s"执行partitionBy后前的分区数：${rdd.getNumPartitions}")
  val rdd1: RDD[(Int, Char)] = rdd.partitionBy(new HashPartitioner(3))
  println(s"执行partitionBy后的分区数：${rdd1.getNumPartitions}")

  val range = rdd.map((tuple: (Int, Char)) => (tuple._1, 1))
  val rdd2 = rdd.partitionBy(new RangePartitioner(3, range))
  println(rdd2.partitioner.get.getPartition(5))
}
