package com.wmm.spark.rdd.operator.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 根据key分区后进行归约聚合(分组和聚合)
 */
object ReduceByKeyDemo extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  //从内存中创建RDD
  val rdd: RDD[(Int, Char)] = sparkContext.makeRDD(List((1, 'a'), (1, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f')))
  val rdd1: RDD[(Int, Char)] = rdd.reduceByKey((c2: Char, c1:Char) => (c1 + c2).toChar)
  rdd1.foreach(println)
}
