package com.hawcore.spark.scala.test3

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author xn025665   
  * @date Create on 2019/5/16 18:48 
  */
object LineCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LineCount").setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("D:\\data\\spark\\tempdata\\hello.txt", 4)

    val pairs = lines.map(f => (f, 1))

    val lineCounts = pairs.reduceByKey(_ + _)

    lineCounts.foreach(f => println(f._1 + ":" + f._2))

  }

}
