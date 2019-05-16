package com.hawcore.spark.scala.test

import org.apache.spark.SparkContext

/**
  * @author xn025665   
  * @date Create on 2019/5/16 12:41 
  */
object TopK {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\base\\hadoop-common-2.6.0-bin")
    val spark = new SparkContext("local", "TopK")
    val inputFile = "D:\\data\\spark\\tempdata\\spark.txt"
    val count = spark.textFile(inputFile)
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((a, b) => {
        a + b
      })
      .collect();
    count.foreach(print)
  }
}
