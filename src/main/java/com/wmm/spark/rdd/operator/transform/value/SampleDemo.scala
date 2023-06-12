package com.wmm.spark.rdd.operator.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * sample demo
 * 对数据进行采样
 * 第一个参数：抽取的数据是否放回，false：不放回
 * 第二个参数：第一个参数为false时代表抽取的几率，范围在[0,1]之间,0：全不取；1：全取；为true时代表重复数据的几率，范围大于等于 0.表示每一个元素被期望抽取到的次数
 * 第三个参数：随机数种子
 */
object SampleDemo extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)
  //从内存中创建RDD
  val rdd: RDD[Int] = sparkContext.parallelize(List(1, 2, 3, 4), 1)
  rdd.sample(withReplacement = false, 0.5).setName("1").foreach(println)
  rdd.sample(withReplacement = true, 0.5).foreach(println)
  rdd.sample(withReplacement = true, 2).foreach(println)
}
