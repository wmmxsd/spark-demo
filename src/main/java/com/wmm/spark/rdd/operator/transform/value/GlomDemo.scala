package com.wmm.spark.rdd.operator.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * glom demo
 * 将同一个分区的数据用相同类型的内存数组包装
 */
object GlomDemo extends App {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  //从内存中创建RDD
  val rdd: RDD[Int] = sparkContext.parallelize(List(1, 2, 3, 4), 2)
  val arrayRdd: RDD[Array[Int]] = rdd.glom()
  //  Array(1)
  //  Array(4)
  //  Array(2)
  //  Array(3)
  arrayRdd.foreach((ints: Array[Int]) => println(ints.mkString("Array(", ",", ")")))
  //  1
  //  3
  //  4
  //  2
  arrayRdd.flatMap((ints: Array[Int]) => ints).foreach(println)
}
