package com.hawcore.spark.scala.test3

import java.util
import java.util.{Arrays, List}

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

    //    flatMap(sc)

    //    groupByKey(sc)

    //    reduceByKey(sc)

    //    sortByKey(sc)

    //    join(sc)

    cogroup(sc)

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

  def groupByKey(sc: SparkContext): Unit = {
    val scoreList = Array(new Tuple2[String, Integer]("Class1", 80), new Tuple2[String, Integer]("Class2", 78), new Tuple2[String, Integer]("Class3", 68), new Tuple2[String, Integer]("Class1", 82), new Tuple2[String, Integer]("Class2", 62), new Tuple2[String, Integer]("Class1", 75), new Tuple2[String, Integer]("Class2", 90), new Tuple2[String, Integer]("Class3", 75))

    sc.parallelize(scoreList).groupByKey(6).foreach(line => println(line._1 + ":" + line._2))
  }

  def reduceByKey(sc: SparkContext): Unit = {
    val scoreList = Array(new Tuple2[String, Integer]("Class1", 80), new Tuple2[String, Integer]("Class2", 78), new Tuple2[String, Integer]("Class3", 68), new Tuple2[String, Integer]("Class1", 82), new Tuple2[String, Integer]("Class2", 62), new Tuple2[String, Integer]("Class1", 75), new Tuple2[String, Integer]("Class2", 90), new Tuple2[String, Integer]("Class3", 75))

    sc.parallelize(scoreList).reduceByKey(_ + _).foreach(line => println(line._1 + ":" + line._2))
  }

  def sortByKey(sc: SparkContext): Unit = {
    val scoreList = Array(new Tuple2[String, Integer]("Class1", 80), new Tuple2[String, Integer]("Class2", 78), new Tuple2[String, Integer]("Class3", 68), new Tuple2[String, Integer]("Class1", 82), new Tuple2[String, Integer]("Class2", 62), new Tuple2[String, Integer]("Class1", 75), new Tuple2[String, Integer]("Class2", 90), new Tuple2[String, Integer]("Class3", 75))

    sc.parallelize(scoreList).sortByKey().foreach(line => println(line._1 + ":" + line._2))
  }

  def join(sc: SparkContext): Unit = {
    val scoreList = Array(new Tuple2[Integer, Integer](1, 80), new Tuple2[Integer, Integer](2, 78), new Tuple2[Integer, Integer](3, 68), new Tuple2[Integer, Integer](4, 68))
    val studentList = Array(new Tuple2[Integer, String](1, "zhangsan"), new Tuple2[Integer, String](2, "lisi"), new Tuple2[Integer, String](3, "wangwu"), new Tuple2[Integer, String](5, "liuliu"))

    sc.parallelize(studentList).join(sc.parallelize(scoreList)).foreach(tuple2 => println(tuple2._1 + ":" + tuple2._2))
  }

  def cogroup(sc: SparkContext): Unit = {
    val scoreList = Array(new Tuple2[Integer, Integer](1, 80), new Tuple2[Integer, Integer](1, 75), new Tuple2[Integer, Integer](2, 78), new Tuple2[Integer, Integer](3, 68), new Tuple2[Integer, Integer](4, 68))
    val studentList = Array(new Tuple2[Integer, String](1, "zhangsan"), new Tuple2[Integer, String](1, "zhangsand"), new Tuple2[Integer, String](2, "lisi"), new Tuple2[Integer, String](3, "wangwu"), new Tuple2[Integer, String](5, "liuliu"))

    sc.parallelize(studentList).cogroup(sc.parallelize(scoreList)).foreach(tuple2 => println(tuple2._1 + ":" + tuple2._2))
  }

}
