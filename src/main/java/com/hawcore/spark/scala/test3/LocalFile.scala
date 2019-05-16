package com.hawcore.spark.scala.test3

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author xn025665   
  * @date Create on 2019/5/16 15:20 
  */
object LocalFile {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LocalFile").setMaster("local")

    val sc = new SparkContext(conf);

    val lines = sc.textFile("D:\\data\\spark\\tempdata\\spark.txt")

    val sum = lines.map(line => line.length).reduce(_ + _);

    println(sum)
  }
}
