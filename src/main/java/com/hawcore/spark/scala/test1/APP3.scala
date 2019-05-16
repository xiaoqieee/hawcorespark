package com.hawcore.spark.scala.test1

object APP3 {
  def main(args: Array[String]): Unit = {
    val sayHello: () => Unit = () => println("Hello")

    def g(f: () => Unit, num: Int) = {
      for (i <- 1 to num) f()
    }

    g(sayHello, 3)

    def saySomeThing(str1: String) = (str2: String) => println(str1 + " " + str2)

    val aa: String => Unit = saySomeThing("Spark")
    aa("Hadoop")
  }

}
