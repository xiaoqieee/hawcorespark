package com.hawcore.spark.scala.test3

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author xn025665   
  * @date Create on 2019/5/27 14:54 
  */
object SortWorldCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LocalFile").setMaster("local")

    val sc = new SparkContext(conf);

    val lines = sc.textFile("D:\\data\\spark\\tempdata\\spark.txt")

    lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).map(tuple => (tuple._2, tuple._1)).sortByKey(false).map(tuple => (tuple._2, tuple._1)).foreach(tuple => println(tuple._1 + ":" + tuple._2))
  }
}
