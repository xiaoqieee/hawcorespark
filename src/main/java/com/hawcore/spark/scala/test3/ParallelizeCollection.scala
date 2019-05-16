package com.hawcore.spark.scala.test3

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author xn025665   
  * @date Create on 2019/5/16 12:54 
  */
object ParallelizeCollection {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local")

    val sc = new SparkContext(conf)

    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val numberRDD = sc.parallelize(numbers, 5)

    var sum = numberRDD.reduce(_ + _)

    println(sum)


  }
}
