package test

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\base\\hadoop-common-2.6.0-bin")
    val inputFile = "D:\\data\\spark\\tempdata\\spark.txt"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    val start = System.currentTimeMillis()
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCount.foreach(println)
    println("总耗时：" + (System.currentTimeMillis()-start))
    wordCount.saveAsTextFile("D:\\data\\spark\\tempdata\\resule.txt")
  }
}

