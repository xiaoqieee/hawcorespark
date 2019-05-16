package com.hawcore.spark.scala.test3

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author xn025665   
  * @date Create on 2019/5/16 19:59 
  */
object TransformationOperation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TransformationOperation").setMaster("local")

    val sc = new SparkContext(conf)

    //    map(sc)

    //    filter(sc)

    flatMap(sc)
  }

  def map(sc: SparkContext): Unit = {

    val numbers = Array(1, 2, 3, 4, 5)
    sc.parallelize(numbers, 2).map(v => v * 2).foreach(println(_))
  }

  def filter(sc: SparkContext): Unit = {

    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    sc.parallelize(numbers, 2).filter(v => v % 2 == 0).foreach(println(_))
  }

  def flatMap(sc: SparkContext): Unit = {

    val lineList = Array("hello you", "hello me", "hello world")
    sc.parallelize(lineList, 2).flatMap(s => s.split(" ")).foreach(println(_))
  }

}
