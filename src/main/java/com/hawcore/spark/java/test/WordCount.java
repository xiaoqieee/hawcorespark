package com.hawcore.spark.java.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author xn025665
 * @date Create on 2019/5/16 15:38
 */
public class WordCount {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "D:\\base\\hadoop-common-2.6.0-bin");
        String inputFile = "D:\\data\\spark\\tempdata\\spark.txt";
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        JavaRDD<String> lines = sc.textFile(inputFile);
//
//        JavaRDD<String> wordRDD = lines.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) throws Exception {
//                return Arrays.asList(s.split(" ")).iterator();
//            }
//        });
//        JavaPairRDD<String, Integer> wordCountRDD = wordRDD.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String s) throws Exception {
//                return new Tuple2<String, Integer>(s, 1);
//            }
//        });
//        JavaPairRDD<String, Integer> countRdd = wordCountRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//                return integer + integer;
//            }
//        });
//
//
//        countRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
//            @Override
//            public void call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {
//                while (tuple2Iterator.hasNext()) {
//                    Tuple2<String, Integer> tuple2 = tuple2Iterator.next();
//                    System.out.println(tuple2._1 + ":" + tuple2._2);
//                }
//            }
//        });
        sc.textFile(inputFile).flatMap(line -> Arrays.asList(line.split(" ")).iterator()).mapToPair(k -> new Tuple2<>(k, 1)).reduceByKey((x, y) -> x + y).foreach(t -> System.out.println(t._1 + ":" + t._2));


    }
}
