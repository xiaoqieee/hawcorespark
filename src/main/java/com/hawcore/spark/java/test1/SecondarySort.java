package com.hawcore.spark.java.test1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * @author xn025665
 * @date Create on 2019/5/27 15:17
 */
public class SecondarySort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LocalFile").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("D:\\data\\spark\\tempdata\\sort.txt");

        JavaPairRDD<MySecondarySortKey, String> pairs = lines.mapToPair(new PairFunction<String, MySecondarySortKey, String>() {
            @Override
            public Tuple2<MySecondarySortKey, String> call(String s) throws Exception {
                return new Tuple2<>(new MySecondarySortKey(Integer.valueOf(s.split(" ")[0]), Integer.valueOf(s.split(" ")[1])), s);
            }
        });
        pairs.sortByKey().map(new Function<Tuple2<MySecondarySortKey, String>, String>() {
            @Override
            public String call(Tuple2<MySecondarySortKey, String> v1) throws Exception {
                return v1._2;
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });


        lines.mapToPair(s->new Tuple2<>(new MySecondarySortKey(Integer.valueOf(s.split(" ")[0]), Integer.valueOf(s.split(" ")[1])),s)).sortByKey().map(Tuple2::_2).foreach(s->System.out.println(s));

        sc.close();
    }
}
