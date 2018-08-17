package com.bay.sessionanalyze.spark.session;

import scala.Serializable;
import scala.math.Ordered;

/**
 * 自定义排序类
 * 进行排序算法主要几个字段:点击次数,下单次数,支付次数
 * 需要实现Ordered接口中几个方法进行排序
 * 需要和其他几个key进行比较大于,小于,大于等于,小于等于
 * 因为在比较的时候需要把数据进行网络传输,需要序列化
 * <p>
 * Author by BayMin, Date on 2018/8/17.
 */
public class CategorySortKey implements Ordered<CategorySortKey>, Serializable {
    // 点击次数
    private long clickCount;
    // 下单次数
    private long orderCount;
    // 支付次数
    private long payCount;

    public CategorySortKey() {
    }

    public CategorySortKey(long clickCount, long orderCount, long payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    @Override
    public int compare(CategorySortKey that) {
        if (this.clickCount - that.clickCount != 0)
            return (int) (this.clickCount - that.clickCount);
        else if (this.orderCount - that.orderCount != 0)
            return (int) (this.orderCount - that.orderCount);
        else if (this.payCount - that.payCount != 0)
            return (int) (this.payCount - that.payCount);
        return 0;
    }

    @Override
    public boolean $less(CategorySortKey that) {
        if (this.clickCount < that.clickCount)
            return true;
        else if (this.clickCount == that.clickCount && this.orderCount < that.orderCount)
            return true;
        else if (this.clickCount == that.clickCount && this.orderCount == that.orderCount && this.payCount < that.payCount)
            return true;
        return false;
    }

    @Override
    public boolean $greater(CategorySortKey that) {
        if (this.clickCount > that.clickCount)
            return true;
        else if (this.clickCount == that.clickCount && this.orderCount > that.orderCount)
            return true;
        else if (this.clickCount == that.clickCount && this.orderCount == that.orderCount && this.payCount > that.payCount)
            return true;
        return false;
    }

    @Override
    public boolean $less$eq(CategorySortKey that) {
        if ($less(that))
            return true;
        else if (this.clickCount == that.clickCount && this.orderCount == that.orderCount && this.payCount == that.payCount)
            return true;
        return false;
    }

    @Override
    public boolean $greater$eq(CategorySortKey that) {
        if ($greater(that))
            return true;
        else if (this.clickCount == that.clickCount && this.orderCount == that.orderCount && this.payCount == that.payCount)
            return true;
        return false;
    }

    @Override
    public int compareTo(CategorySortKey that) {
        return compare(that);
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public long getPayCount() {
        return payCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }
}
