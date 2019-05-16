package com.hawcore.spark.scala.test1

object APP2 {

  def main(args: Array[String]): Unit = {
    val list = List.range(0, 10);

    val list2 = list.filter((x: Int) => x % 2 == 0)
    println(list2.mkString(","))

    val list3 = list.filter(x => x % 2 == 1)
    println(list3.mkString(","))

    val list4 = list.filter(_ % 2 == 0)
    println(list4.mkString(","))
  }
}
