package com.wmm.spark.rdd.operator.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 最通用的对 key-value 型 rdd 进行聚集操作的聚集函数（aggregation function）。
 * 类似于aggregate()，combineByKey()允许用户返回值的类型与输入不一致。
 */
object CombineByKeyDemo extends App {
  //求每个key的平均值
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  val rdd: RDD[(String, Int)] = sparkContext.makeRDD(List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)
  val combineRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
    (_, 1),
    (value: (Int, Int), nextValue) => (value._1 + nextValue, value._2 + 1),
    (value1: (Int, Int), value2: (Int, Int)) => (value1._1 + value2._1, value1._2 + value2._2)
  )
  combineRDD.foreach(println)
  val avgRDD: RDD[(String, Int)] = combineRDD.map((tuple: (String, (Int, Int))) => (tuple._1, tuple._2._1 / tuple._2._2))
  avgRDD.foreach(println)
}
