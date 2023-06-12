package com.wmm.spark.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ActionOperatorsDemo extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)

  //reduce：聚集 RDD 中的所有元素，先聚合分区内数据，再聚合分区间数据
  val sourceRDD1: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4))
  val reduce: Int = sourceRDD1.reduce(_ * _)
  println(reduce)

  //collect：以数组 Array 的形式返回数据集的所有元素
  val array: Array[Int] = sourceRDD1.collect()
  println(array.mkString("Array(", ", ", ")"))

  //count：返回RDD中元素的数量。
  val count: Long = sourceRDD1.count()
  println("count:" + count)

  //first: 返回此RDD中的第一个元素
  val first: Int = sourceRDD1.first()
  println("first:" + first)

  //take:返回一个由 RDD 的前 n 个元素组成的数组
  val takeArray: Array[Int] = sourceRDD1.take(3)
  println("takeArray:" + takeArray.mkString("Array(", ", ", ")"))

  //takeOrdered:返回该 RDD 排序后的前 n 个元素组成的数组
  val takeOrderedArray: Array[Int] = sourceRDD1.takeOrdered(3)(Ordering.Int.reverse)
  println("takeOrderedArray:" + takeOrderedArray.mkString("Array(", ", ", ")"))

  //aggregate:分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
  //output: 10
  val aggregateResult: Int = sourceRDD1.aggregate(0)(_ + _, _ + _)
  //output：60
  val aggregateResult1: Int = sourceRDD1.aggregate(10)(_ + _, _ + _)
  println("aggregateResult:" + aggregateResult)
  println("aggregateResult1:" + aggregateResult1)

  //fold:分区内计算规则和分区间计算规则相同时，aggregate 就可以简化为 fold
  val foldResult: Int = sourceRDD1.fold(0)(_ + _)
  println("foldResult:" + foldResult)

  //countByKey:统计每种key的元素个数
  val sourceRDD2: RDD[(Int, String)] = sparkContext.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))
  val countByKeyRDD: collection.Map[Int, Long] = sourceRDD2.countByKey()
  countByKeyRDD.foreach(println)

  //countByValue:统计每一个唯一值
  val countByValueRDD: collection.Map[(Int, String), Long] = sourceRDD2.countByValue()
  countByValueRDD.foreach(println)

  sourceRDD2.saveAsTextFile("output1")
  sourceRDD2.saveAsObjectFile("output2")
  sourceRDD2.saveAsSequenceFile("output3")
}
