package com.wmm.spark.rdd.operator.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, TaskContext}

/**
 * 根据key分区(分组聚合)
 */
object AggregateByKeyDemo extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
  val sparkContext = new SparkContext(sparkConf)

  aggregateByKey1()

  aggregateByKey2()

  private def aggregateByKey1(): Unit = {
    //从内存中创建RDD
    val rdd: RDD[(String, Char)] = sparkContext.makeRDD(List(("jordan", 'a'), ("jordan", 'b'), ("anthony", 'c'), ("anthony", 'd'), ("anthony", 'e'), ("smith", 'f')))
    //计算不同名字出现的次数
    //(_, _) => 1，每个元素都返回1
    //_ + _，累加器逻辑，将累加值和上面返回的1相加
    val value: RDD[(String, Int)] = rdd.aggregateByKey(0)((_, _) => 1, _ + _)
    value.foreach(println)
  }

  private def aggregateByKey2(): Unit = {
    // 取出每个分区内相同 key 的最大值然后分区间相加
    // aggregateByKey 算子是函数柯里化，存在两个参数列表
    // 1. 第一个参数列表中的参数表示初始值
    // 2. 第二个参数列表中含有两个参数
    // 2.1 第一个参数表示分区内的计算规则
    // 2.2 第二个参数表示分区间的计算规则
    val rdd1: RDD[(String, Int)] = sparkContext.makeRDD(List(("a", 1), ("a", 2), ("c", 3), ("b", 4), ("c", 5), ("c", 6)), 2)
    rdd1.foreach((tuple: (String, Int)) => {
      println(s"${TaskContext.getPartitionId()}:${tuple._1},${tuple._2}")
    })
    // 分区0:("a", 1), ("a", 2), ("c", 3) => ("a", 2), ("c", 3) => ("a",2) ("b",4) ("c",9)
    // 分区1:("b", 4), ("c", 5), ("c", 6) => ("b", 4), ("c", 6)
    /*val resultRDD = rdd1.aggregateByKey(0, new HashPartitioner(2))(math.max, _ + _)
    resultRDD.foreach((tuple: (String, Int)) => {
      println(s"${TaskContext.getPartitionId()}:${tuple._1},${tuple._2}")
    })*/
    val resultRDD: RDD[(String, Int)] = rdd1.aggregateByKey(0)(math.max, _ + _)
    resultRDD.foreach((tuple: (String, Int)) => {
      println(s"${TaskContext.getPartitionId()}:${tuple._1},${tuple._2}")
    })
  }
}
