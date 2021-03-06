package com.hawcore.spark.java.test1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * @author xn025665
 * @date Create on 2019/5/16 11:41
 */
public class ParallelizeCollection {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

//        int sum = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer a, Integer b) throws Exception {
//                return a + b;
//            }
//        });

        int sum = numberRDD.reduce((a, b) -> a + b);


        System.out.println("1~10累加和: " + sum);

        sc.close();

    }
}
