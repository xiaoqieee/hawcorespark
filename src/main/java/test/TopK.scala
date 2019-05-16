package test

import org.apache.spark.{SparkContext}

object TopK {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\base\\hadoop-common-2.6.0-bin")
    val spark = new SparkContext("local","TopK")
    val inputFile = "D:\\data\\spark\\tempdata\\spark.txt"
    val count = spark.textFile(inputFile).flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey((a,b)=>{
      a+b
    }).collect();
    count.foreach(print)
  }
}
