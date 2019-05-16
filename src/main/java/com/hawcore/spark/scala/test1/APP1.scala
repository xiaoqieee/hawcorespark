package com.hawcore.spark.scala.test1

object APP1 {
  def main(args: Array[String]): Unit = {
    def fun(str1: String, str2: String) = {
      str1 + " " + str2;
    }

    println(fun("hello", "world"))

    val add = (str1: String, str2: String) => str1 + " " + str2
    println(add("hello", "world"))

    def g(f:(String, String) =>String, str1:String, str2:String) = {
      println(f(str1,str2))
    }
    g(add,"hello","world")
  }
}
