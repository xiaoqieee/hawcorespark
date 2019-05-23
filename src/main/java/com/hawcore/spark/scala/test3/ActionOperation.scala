package com.hawcore.spark.scala.test3

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author xn025665   
  * @date Create on 2019/5/22 14:40 
  */
object ActionOperation {



  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ActionOperation").setMaster("local")

    val sc = new SparkContext(conf)


    reduce(sc)

    collect(sc)

    cout(sc)

  }

  def reduce(sc: SparkContext): Unit = {
    val numberList: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    println(sc.parallelize(numberList).reduce(_ + _))
  }

  def collect(sc: SparkContext): Unit = {
    val numberList: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    sc.parallelize(numberList).collect().foreach(f=>println(f))
  }

  def cout(sc: SparkContext): Unit = {
    val numberList: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    println(sc.parallelize(numberList).count())
  }
}
