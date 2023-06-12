package com.wmm.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
public class WorldCountDemo {
    public static void main(String[] args) {
        //创建配置类
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("workCount");
        //创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //读取文件
        JavaRDD<String> context = sc.textFile(Objects.requireNonNull(WorldCountDemo.class.getResource("/word.txt")).getPath());
        //将所有行的单词提取出来
        JavaRDD<String> word = context.flatMap((FlatMapFunction<String, String>) line -> {
            List<String> list = Arrays.asList(line.split(" "));
            return list.iterator();
        });
        //String转换为<String, 1>，代表单词及其出现的次数，默认为一次。
        JavaRDD<Tuple2<String, Integer>> wordAndCount = word.map((Function<String, Tuple2<String, Integer>>) v1 -> Tuple2.apply(v1, 1));
        //按照单词进行分组，便于统计每个单词出现的次数。
        JavaPairRDD<String, Tuple2<String, Integer>> keyRdd = wordAndCount.keyBy((Function<Tuple2<String, Integer>, String>) v1 -> v1._1);
        //单词每出现一次，次数就+1
        JavaPairRDD<String, Tuple2<String, Integer>> sumRdd = keyRdd
                .reduceByKey((Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>) (v1, v2) -> Tuple2.apply(v1._1, v1._2 + v2._2));
        //不同分组的元素合并到一个组，便于后续统一处理
        List<Tuple2<String, Tuple2<String, Integer>>> result = sumRdd.collect();
        //打印
        result.forEach(System.out::println);
        //关闭 Spark 连接
        sc.stop();
    }
}
