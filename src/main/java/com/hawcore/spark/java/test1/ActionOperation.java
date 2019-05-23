package com.hawcore.spark.java.test1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * @author xn025665
 * @date Create on 2019/5/22 14:36
 */
public class ActionOperation {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ActionOperation").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);


        sc.close();
    }

    private static void reduce(JavaSparkContext sc) {
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        Integer sum = numbers.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        System.out.println(sum);

        System.out.println(numbers.reduce((v1, v2) -> v1 + v2));
    }
}
