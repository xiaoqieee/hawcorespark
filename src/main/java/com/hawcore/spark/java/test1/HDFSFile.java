package com.hawcore.spark.java.test1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author xn025665
 * @date Create on 2019/5/16 13:38
 */
public class HDFSFile {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("HDFSFile");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("hdfs:/spark.txt");
//        JavaRDD<Integer> lineLengthRDD = lines.map(new Function<String, Integer>() {
//            @Override
//            public Integer call(String s) throws Exception {
//                return s.length();
//            }
//        });


        JavaRDD<Integer> lineLengthRDD = lines.map(s -> s.length());

//        int sum = lineLengthRDD.reduce(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//                return integer + integer2;
//            }
//        });
        int sum = lineLengthRDD.reduce((v1, v2) -> v1 + v2);

        System.out.println(sum);
        sc.close();
    }
}
