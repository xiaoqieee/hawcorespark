package com.hawcore.spark.java.test1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author xn025665
 * @date Create on 2019/5/27 15:43
 */
public class Top3 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SortWorldCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(3, 4, 2, 7, 1, 6, 3, 8, 1, 3, 9, 4);

        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        JavaPairRDD<Integer, Integer> pairs = numbers.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<>(integer, integer);
            }
        });
        JavaPairRDD<Integer, Integer> sortedPairs = pairs.sortByKey(false);
        JavaRDD<Integer> sortedNumbers = sortedPairs.map(new Function<Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, Integer> v1) throws Exception {
                return v1._2;
            }
        });

        List<Integer> top3 = sortedNumbers.take(3);
        for (Integer a : top3) {
            System.out.println(a);
        }

        for (Integer integer : numbers.mapToPair(l -> new Tuple2<>(l, l)).sortByKey(false).map(s -> s._2).take(3)) {
            System.out.println(integer);
        }

        sc.close();
    }
}
