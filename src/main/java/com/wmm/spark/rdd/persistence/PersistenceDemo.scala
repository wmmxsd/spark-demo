package com.wmm.spark.rdd.persistence

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD 持久化
 * 1.Cache缓存：
 * RDD 通过 Cache 或者 Persist 方法将前面的计算结果缓存，默认情况下会把数据以缓存在 JVM 的堆内存中。
 * 但是并不是这两个方法被调用时立即缓存，而是触发后面的 action 算子时，该 RDD 将会被缓存在计算节点的内存中，并供后面重用。
 * 缓存有可能丢失，或者存储于内存的数据由于内存不足而被删除，RDD 的缓存容错机制保证了即使缓存丢失也能保证计算的正确执行。
 * 通过基于 RDD 的一系列转换，丢失的数据会被重算，由于 RDD 的各个 Partition 是相对独立的，因此只需要计算丢失的部分即可，并不需要重算全部 Partition。
 *
 * 2.检查点checkpoint
 * 所谓的检查点其实就是通过将 RDD 中间结果写入磁盘，由于血缘依赖过长会造成容错成本过高，这样就不如在中间阶段做检查点容错，如果检查点之后有节点出现问题，可以从检查点开始重做血缘，减少了开销。
 * 对 RDD 进行 checkpoint 操作并不会马上被执行，必须执行 Action 操作才能触发。
 *
 * 缓存和检查点区别
 * 1）Cache 缓存只是将数据保存起来，不切断血缘依赖。Checkpoint 检查点切断血缘依赖。
 * 2）Cache 缓存的数据通常存储在磁盘、内存等地方，可靠性低。Checkpoint 的数据通常存储在 HDFS 等容错、高可用的文件系统，可靠性高。
 * 3）建议对 checkpoint()的 RDD 使用 Cache 缓存，这样 checkpoint 的 job 只需从 Cache 缓存中读取数据即可，否则需要再从头计算一次 RDD。
 */
object PersistenceDemo extends App {
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("advClickEvent")
  val sparkContext: SparkContext = new SparkContext(conf)
  //设置检查点路径
  sparkContext.setCheckpointDir("/checkpoint1")

  val logTextRDD: RDD[String] = sparkContext.textFile(this.getClass.getResource("/click-events.log").getPath)

  val advClickEventRDD: RDD[((String, String), Int)] = logTextRDD.map(log => {
    val logArr: Array[String] = log.split(" ")
    ((logArr(1), logArr(4)), 1)
  })
  //增加缓存,避免再重新跑一个 job 做 checkpoint
  advClickEventRDD.cache()
  // 数据检查点：针对 advClickEventRDD 做检查点计算
  advClickEventRDD.checkpoint()

  advClickEventRDD.collect().foreach(println)
}