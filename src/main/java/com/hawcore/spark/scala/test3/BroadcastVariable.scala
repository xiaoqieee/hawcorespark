package com.hawcore.spark.scala.test3

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author xn025665   
  * @date Create on 2019/5/27 14:20 
  */
object BroadcastVariable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LineCount").setMaster("local")

    val sc = new SparkContext(conf)

    val factor = 3;

    val broadcast = sc.broadcast(factor);

    val numberList: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)


    numberList.map(_ * factor).foreach(println(_))
    numberList.map(_ * broadcast.value).foreach(println(_))
  }
}
