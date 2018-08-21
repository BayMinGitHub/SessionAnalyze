package com.bay.sessionanalyze.spark.product;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.api.java.UDF2;

/**
 * UDF2<String, String, String>
 * 前两个类型是传入的值的类型
 * 第一个类型是指:json格式的字符串
 * 第二个类型是指:要获取json字符串中的字段名
 * 第三个类型是指:返回的类型
 * <p>
 * Author by BayMin, Date on 2018/8/21.
 */
public class GetJsonObjectUDF implements UDF2<String, String, String> {
    @Override
    public String call(String s, String s2) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(s);
        return jsonObject.getString(s2);
    }
}
