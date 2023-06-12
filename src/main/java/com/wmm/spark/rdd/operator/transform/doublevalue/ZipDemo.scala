package com.wmm.spark.rdd.operator.transform.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 以一个 RDD 元素为主，去除两个 RDD 中重复元素，将其他元素保留下来。求差集
 */
object ZipDemo extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  zip1()
  //  zip2()
  zip3()


  private def zip1(): Unit = {
    //从内存中创建RDD
    val rdd1: RDD[Any] = sparkContext.parallelize(List(1, 2, 3, 4, "j"), 2)
    val rdd2: RDD[Any] = sparkContext.parallelize(List(9, 8, 2, "j", '2'), 2)
    //两个rdd元素个数一样，分区数一样
    val rdd = rdd1.zip(rdd2)
    //  (3, 2)
    //  (4, j)
    //  (j, 2)
    //  (1, 9)
    //  (2, 8)
    rdd.foreach(println)
  }

  private def zip2(): Unit = {
    val rdd3: RDD[Any] = sparkContext.parallelize(List(1, 2, 3, 4), 2)
    val rdd4: RDD[Any] = sparkContext.parallelize(List(9, 8, 2, "j", '2'), 2)
    //两个rdd的分区数一样但元素个数不一样
    val rdd5: RDD[(Any, Any)] = rdd3.zip(rdd4)
    //报错：org.apache.spark.SparkException: Can only zip RDDs with same number of elements in each partition
    rdd5.foreach(println)
  }

  private def zip3(): Unit = {
    val rdd6: RDD[Any] = sparkContext.parallelize(List(1, 2, 3, 4, 5), 3)
    val rdd7: RDD[Any] = sparkContext.parallelize(List(9, 8, 2, "j", '2'), 2)
    //两个rdd的元素个数一样但分区数不一样
    val rdd8: RDD[(Any, Any)] = rdd6.zip(rdd7)
    //报错：Exception in thread "main" java.lang.IllegalArgumentException: Can't zip RDDs with unequal numbers of partitions: List(3, 2)
    rdd8.foreach(println)
  }
}
