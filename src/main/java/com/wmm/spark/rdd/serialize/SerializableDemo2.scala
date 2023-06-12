package com.wmm.spark.rdd.serialize

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
Java 的序列化能够序列化任何的类。但是比较重（字节多），序列化后，对象的提交也
比较大。Spark 出于性能的考虑，Spark2.0 开始支持另外一种 Kryo 序列化机制。Kryo 速度
是 Serializable 的 10 倍。当 RDD 在 Shuffle 数据的时候，简单数据类型、数组和字符串类型
已经在 Spark 内部使用 Kryo 来序列化。
注意：即使使用 Kryo 序列化，也要继承 Serializable 接口。
* */
object SerializableDemo2 {
  def main(args: Array[String]): Unit = {
    //1.创建 SparkConf 并设置 App 名称
    val conf: SparkConf = new SparkConf()
      .setAppName("SparkCoreTest")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册需要使用kryo序列化的自定义类
      .registerKryoClasses(Array(classOf[Search]))
    //2.创建 SparkContext，该对象是提交 Spark App 的入口
    val sc: SparkContext = new SparkContext(conf)
    //3.创建一个 RDD
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))
    //3.1 创建一个 Search 对象
    val search = new Search("hello")
    search.getMatch1(rdd).collect().foreach(println)
    search.getMatch2(rdd).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }

  class Search(query: String) extends Serializable {
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

