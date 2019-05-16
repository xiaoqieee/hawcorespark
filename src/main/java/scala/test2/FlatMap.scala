package scala.test2

import org.apache.spark.{SparkConf, SparkContext}

object FlatMap {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\base\\hadoop-common-2.6.0-bin")
    val conf = new SparkConf().setMaster("local").setAppName("Map")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 5)
    val fm = rdd.map(x => (1 to x)).collect()
    fm.foreach(x => println(x + " "))
  }
}
