package com.hawcore.spark.java.test1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * @author xn025665
 * @date Create on 2019/5/27 13:43
 */
public class Persist {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Persist").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("D:\\data\\spark\\tempdata\\spark.txt");
//        lines.persist(StorageLevel.MEMORY_AND_DISK());
        long start = System.currentTimeMillis();
        System.out.println(lines.count());
        System.out.println(System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        System.out.println(lines.count());
        System.out.println(System.currentTimeMillis() - start);


        sc.close();
    }


}
