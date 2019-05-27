package com.hawcore.spark.scala.test3

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author xn025665   
  * @date Create on 2019/5/27 14:34 
  */
object AccumulatorVariable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LineCount").setMaster("local")

    val sc = new SparkContext(conf)

    val sumAcc = sc.accumulator(0);

    val numberList: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)


    sc.parallelize(numberList).foreach(sumAcc.add(_))

    println(sumAcc.value)
  }
}
