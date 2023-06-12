package com.wmm.spark.rdd.operator.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将数据根据指定的规则进行分组, 分区默认不变，但是数据会被打乱重新组合，我们将这样的操作称之为 shuffle。极限情况下，数据可能被分在同一个分区中
 */
object GroupByDemo extends App {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  //从内存中创建RDD
  val rdd: RDD[Int] = sparkContext.parallelize(List(1, 2, 3, 4), 1)
  val groupRdd: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 2)
  //output
  //  (0, CompactBuffer(2, 4))
  //  (1, CompactBuffer(1, 3))
  groupRdd.foreach(println)

  val rdd1: RDD[String] = sparkContext.makeRDD(List("Hello", "hive", "hbase", "Hadoop"))
  //output
  //  (h,CompactBuffer(hive, hbase))
  //  (H,CompactBuffer(Hello, Hadoop))
  rdd1.groupBy((str: String) => str.charAt(0)).foreach(println)
}
