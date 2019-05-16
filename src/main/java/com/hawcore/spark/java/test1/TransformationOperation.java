package com.hawcore.spark.java.test1;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author xn025665
 * @date Create on 2019/5/16 19:52
 */
public class TransformationOperation {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TransformationOperation").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        map(sc);

//        filter(sc);

//        flatMap(sc);

        groupByKey(sc);

        sc.close();
    }

    private static void map(JavaSparkContext sc) {

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

//        JavaRDD<Integer> multipleNumberRDD = numberRDD.map(new Function<Integer, Integer>() {
//            @Override
//            public Integer call(Integer integer) throws Exception {
//                return integer * 2;
//            }
//        });
//        multipleNumberRDD.foreach(new VoidFunction<Integer>() {
//            @Override
//            public void call(Integer integer) throws Exception {
//                System.out.println(integer);
//            }
//        });

        numberRDD.map(v -> v * 2).foreach(s -> System.out.println(s));
    }

    private static void filter(JavaSparkContext sc) {
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers, 5);

//        JavaRDD<Integer> evenNumberRDD = numberRDD.filter(new Function<Integer, Boolean>() {
//            @Override
//            public Boolean call(Integer integer) throws Exception {
//                return integer % 2 == 0;
//            }
//        });
//        evenNumberRDD.foreach(new VoidFunction<Integer>() {
//            @Override
//            public void call(Integer integer) throws Exception {
//                System.out.println(integer);
//            }
//        });

        numberRDD.filter(v -> v % 2 == 0).foreach(v -> System.out.println(v));
    }

    private static void flatMap(JavaSparkContext sc) {

        List<String> lineList = Arrays.asList("hello you", "hello me", "hello world");
        JavaRDD<String> lines = sc.parallelize(lineList);

//        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) throws Exception {
//                return Arrays.asList(s.split(" ")).iterator();
//            }
//        });
//
//        words.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });

        lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator()).foreach(s -> System.out.println(s));
    }

    private static void groupByKey(JavaSparkContext sc) {
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<>("Class1", 80),
                new Tuple2<>("Class2", 78),
                new Tuple2<>("Class3", 68),
                new Tuple2<>("Class1", 82),
                new Tuple2<>("Class2", 62),
                new Tuple2<>("Class1", 75),
                new Tuple2<>("Class2", 90),
                new Tuple2<>("Class3", 75)
        );
        JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList);
        JavaPairRDD<String, Iterable<Integer>> classScores = scores.groupByKey();
        classScores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
                System.out.println(tuple2._1 + ":" + tuple2._2.iterator());
            }
        });

    }
}
