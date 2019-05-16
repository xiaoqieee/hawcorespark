package com.hawcore.spark.java.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * @author xn025665
 * @date Create on 2019/5/16 18:38
 */
public class LineCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("D:\\data\\spark\\tempdata\\hello.txt");

        JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> lineCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        lineCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2._1 + ":" + tuple2._2);
            }
        });

        lines.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((v1, v2) -> v1 + v2).foreach(tuple2 -> System.out.println(tuple2._1 + ":" + tuple2._2));

        sc.close();
    }
}
