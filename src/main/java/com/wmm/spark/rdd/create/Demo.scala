package com.wmm.spark.rdd.create

import org.apache.spark.{SparkConf, SparkContext}

object Demo extends App {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  //从内存中创建RDD
  val rdd1 = sparkContext.parallelize(List(1, 2, 3, 4), 2)
  val rdd2 = sparkContext.makeRDD(List(1, 2, 3, 4), 2)
  rdd1.foreach(println)
  rdd2.foreach(println)
  //从外部存储（文件）创建 RDD
  val rdd3 = sparkContext.textFile(this.getClass.getResource("/word.txt").getPath)
  rdd3.foreach(println)
}
