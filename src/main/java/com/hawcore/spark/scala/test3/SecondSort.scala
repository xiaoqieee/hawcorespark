package com.hawcore.spark.scala.test3

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author xn025665   
  * @date Create on 2019/5/27 15:35 
  */
object SecondSort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LocalFile").setMaster("local")

    val sc = new SparkContext(conf);

    val lines = sc.textFile("D:\\data\\spark\\tempdata\\sort.txt")

    lines.map(l => (new SecondarySortKey(l.split(" ")(0).toInt, l.split(" ")(1).toInt), l)).sortByKey().map(m => m._2).foreach(println(_))
  }
}
