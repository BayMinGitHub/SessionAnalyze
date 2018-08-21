package com.bay.sessionanalyze.spark.product;


import org.apache.spark.sql.api.java.UDF3;

/**
 * 将两个字段使用指定分隔符拼接起来
 * UDF3<Long, String, String, String> 中的几个类型分别代表:
 * 前两个类型是指调用者传进来的拼接字段
 * 第三个是指用于拼接的分隔符
 * 第四个是指返回类型
 * Author by BayMin, Date on 2018/8/21.
 */
public class ConcatLongStringUDF implements UDF3<Long, String, String, String> {
    @Override
    public String call(Long aLong, String s, String s2) throws Exception {
        return String.valueOf(aLong) + s2 + s;
    }
}

