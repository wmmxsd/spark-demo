package com.wmm.spark.rdd.serialize

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/*
* 从计算的角度, 算子以外的代码都是在 Driver 端执行, 算子里面的代码都是在 Executor
*端执行。那么在 scala 的函数式编程中，就会导致算子内经常会用到算子外的数据，这样就
*形成了闭包的效果，如果使用的算子外的数据无法序列化，就意味着无法传值给 Executor
*端执行，就会发生错误，所以需要在执行任务计算前，检测闭包内的对象是否可以进行序列
*化，这个操作我们称之为闭包检测。Scala2.12 版本后闭包编译方式发生了改变
* */
object SerializableDemo1 {
  def main(args: Array[String]): Unit = {
    //1.创建 SparkConf 并设置 App 名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //2.创建 SparkContext，该对象是提交 Spark App 的入口
    val sc: SparkContext = new SparkContext(conf)
    //3.创建一个 RDD
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))
    //3.1 创建一个 Search 对象
    val search1 = new Search1("hello")
    search1.getMatch1(rdd).collect().foreach(println)
    //3.3 属性传递，打印：ERROR Task not serializable
    search1.getMatch2(rdd).collect().foreach(println)

    val search = new Search("hello")
    //3.2 函数传递，打印：ERROR Task not serializable
    search.getMatch1(rdd).collect().foreach(println)
    //3.3 属性传递，打印：ERROR Task not serializable
    search.getMatch2(rdd).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }

  class Search(query: String) {
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    // 函数序列化案例
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      rdd.filter(x => x.contains(query))
    }
  }

  class Search1(query: String) extends Serializable {
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    // 函数序列化案例
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      rdd.filter(x => x.contains(query))
    }
  }
}

