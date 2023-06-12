package com.wmm.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WorldCount extends App {
  //创建 Spark 运行配置对象
  val conf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
  //创建Spark上下文环境对象（连接对象）
  val sparkContext: SparkContext = new SparkContext(conf)
  //读取文件数据
  val fileRdd: RDD[String] = sparkContext.textFile(this.getClass.getResource("/word.txt").getPath)
  //分词
  val wordRdd: RDD[String] = fileRdd.flatMap(_.split(" "))
  //String转换为<String, 1>，代表单词及其出现的次数，默认为一次
  val tempWordAndCountRdd: RDD[(String, Int)] = wordRdd.map((_, 1))
  //单词每出现一次，次数就+1
  val wordAndCountRdd : RDD[(String, Int)]= tempWordAndCountRdd.reduceByKey(_ + _)
  // 将数据聚合结果采集到内存中
  val word2Count: Array[(String, Int)] = wordAndCountRdd.collect()
  // 打印结果
  word2Count.foreach(println)
  //关闭 Spark 连接
  sparkContext.stop()
}
