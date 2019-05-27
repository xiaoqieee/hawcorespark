package com.hawcore.spark.java.test1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.dmg.pmml.SeasonalTrendDecomposition;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author xn025665
 * @date Create on 2019/5/27 14:37
 */
public class SortWorldCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SortWorldCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("D:\\data\\spark\\tempdata\\spark.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        JavaPairRDD<Integer, String> countWords = wordCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return new Tuple2<Integer, String>(tuple2._2, tuple2._1);
            }
        });
        JavaPairRDD<Integer, String> sortedCountWords = countWords.sortByKey(false);
        JavaPairRDD<String, Integer> sortedWordCounts = sortedCountWords.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return new Tuple2<>(integerStringTuple2._2, integerStringTuple2._1);
            }
        });
        sortedWordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2._1 + ":" + tuple2._2);
            }
        });

        lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(v -> new Tuple2<>(v, 1))
                .reduceByKey((v1, v2) -> v1 + v2)
                .mapToPair(tuple2 -> new Tuple2<>(tuple2._2, tuple2._1))
                .sortByKey(false)
                .mapToPair(tuple2 -> new Tuple2<>(tuple2._2, tuple2._1))
                .foreach(tuple2 -> System.out.println(tuple2._1 + ":" + tuple2._2));

        sc.close();
    }
}
