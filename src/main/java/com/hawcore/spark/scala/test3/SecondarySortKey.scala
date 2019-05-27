package com.hawcore.spark.scala.test3

/**
  * @author xn025665   
  * @date Create on 2019/5/27 15:32 
  */
class SecondarySortKey(val first: Int, val second: Int) extends Ordered[SecondarySortKey] with Serializable {

  override def compare(that: SecondarySortKey): Int = {
    if (this.first != that.first) {
      return this.first - that.first;
    } else {
      this.second - that.second
    }
  }
}
