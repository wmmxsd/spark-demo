package com.wmm.spark.rdd.operator.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
 * 当分区内计算规则和分区间计算规则相同时，aggregateByKey 就可以简化为 foldByKey
 */
object FoldByKeyDemo extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  val rdd: RDD[(String, Int)] = sparkContext.makeRDD(List(("a", 1), ("a", 2), ("c", 3), ("b", 4), ("c", 5), ("c", 6)), 2)
  // 分区0:("a",1),("a",2),("c",3) => (a,3)(c,3) => (a,3)(b,4)(c,14)
  // 分区1:("b",4),("c",5),("c",6) => (b,4)(c,11)
  rdd.foldByKey(0)(_ + _).foreach((tuple: (String, Int)) => {
    println(s"${TaskContext.getPartitionId()}:${tuple._1},${tuple._2}")
  })
}
