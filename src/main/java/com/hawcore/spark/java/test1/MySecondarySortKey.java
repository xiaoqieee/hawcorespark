package com.hawcore.spark.java.test1;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * @author xn025665
 * @date Create on 2019/5/27 15:04
 */
public class MySecondarySortKey implements Ordered<MySecondarySortKey>, Serializable {

    private Integer first;
    private Integer second;

    public MySecondarySortKey() {
    }

    public MySecondarySortKey(Integer first, Integer second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compare(MySecondarySortKey that) {
        if (this.first != that.getFirst()) {
            return this.first - that.getFirst();
        } else {
            return this.second - that.getSecond();
        }
    }

    @Override
    public boolean $less(MySecondarySortKey that) {
        if (this.first < that.getFirst()) {
            return true;
        } else if (this.first == that.getFirst() && this.second < that.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(MySecondarySortKey that) {
        if (this.first > that.getFirst()) {
            return true;
        } else if (this.first == that.getFirst() && this.second > that.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(MySecondarySortKey that) {
        if (this.$less(that)) {
            return true;
        } else if (this.first == that.getFirst() && this.second == that.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(MySecondarySortKey that) {
        if (this.$greater(that)) {
            return true;
        } else if (this.first == that.getFirst() && this.second == that.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(MySecondarySortKey that) {
        if (this.first != that.getFirst()) {
            return this.first - that.getFirst();
        } else {
            return this.second - that.getSecond();
        }
    }

    public Integer getFirst() {
        return first;
    }

    public void setFirst(Integer first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}
