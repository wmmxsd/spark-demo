package com.wmm.spark.rdd.operator.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * 根据key分区(分组)
 */
object GroupByKeyDemo extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  //从内存中创建RDD
  val rdd: RDD[(Int, Char)] = sparkContext.makeRDD(List((1, 'a'), (1, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f')))
  rdd.groupByKey().foreach(println)
  val value: RDD[(Int, Iterable[Char])] = rdd.groupByKey(2)
  value.foreach(println)
  rdd.groupByKey(new HashPartitioner(2)).foreach(println)
}
