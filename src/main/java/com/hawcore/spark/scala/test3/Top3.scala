package com.hawcore.spark.scala.test3

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author xn025665   
  * @date Create on 2019/5/27 15:50 
  */
object Top3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LocalFile").setMaster("local")

    val sc = new SparkContext(conf);

    val numberList: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val numbers = sc.parallelize(numberList,5);

    numbers.map(f=>(f,f)).sortByKey(false).map(f=>f._2).take(3).foreach(println(_))
  }

}
