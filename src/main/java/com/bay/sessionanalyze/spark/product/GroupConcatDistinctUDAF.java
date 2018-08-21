package com.bay.sessionanalyze.spark.product;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * UDAF:聚合函数
 * 组内拼接去重函数
 * <p>
 * Author by BayMin, Date on 2018/8/21.
 */
public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {
    // 指定输入数据的字段与类型
    private StructType inputSchema = DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("CityInfo", DataTypes.StringType, true)));
    // 指定缓冲数据的字段与类型
    private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true)));
    // 指定返回类型
    private DataType dataType = DataTypes.StringType;
    // 指定是否是确定性的
    // 只有指定为true时,即给定相同的输入,总是返回相同的输出
    private boolean deterministic = true;

    @Override
    public StructType inputSchema() {
        return inputSchema;
    }

    @Override
    public StructType bufferSchema() {
        return bufferSchema;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean deterministic() {
        return deterministic;
    }

    /**
     * 初始化
     * 可以认为是你自己在内部指定一个初始值
     */
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, "");
    }

    /**
     * 更新
     * 一个个的将组内字段传进去
     */
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        // 获取到缓冲中已经拼接过的城市信息
        String bufferCityInfo = buffer.getString(0);
        // 刚刚传进来的某个城市的信息
        String cityInfo = input.getString(0);
        // 在这里实现去重
        // 判断之前没有拼接过某个城市的信息,才可以拼接
        // 城市信息:0:北京 1:上海
        if (!bufferCityInfo.contains(cityInfo)) {
            if ("".equals(bufferCityInfo))
                bufferCityInfo += cityInfo;
            else
                bufferCityInfo += "," + cityInfo;
        }
        buffer.update(0, bufferCityInfo);
    }

    /**
     * 合并
     * update操作,可能对一个分组内的部分数据,在某个节点发生的
     * 但是可能一个分组内的数据会分布在多个节点上处理
     * 此时就要用merge操作,将各个节点上分布式拼接好的字符串合并起来
     * 即:合并两个聚合缓冲并将更新后的换重置存储到buffer1中
     */
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        String bufferCityInfo1 = buffer1.getString(0);
        String bufferCityInfo2 = buffer2.getString(0);
        for (String cityInfo : bufferCityInfo2.split(",")) {
            if (!bufferCityInfo1.contains(cityInfo)) {
                if ("".equals(bufferCityInfo1)) {
                    bufferCityInfo1 += cityInfo;
                } else {
                    bufferCityInfo1 += "," + cityInfo;
                }
            }
        }
        buffer1.update(0, bufferCityInfo1);
    }

    /**
     * 根据给定的聚合缓冲,计算最终结果
     * 一般作为返回
     */
    @Override
    public Object evaluate(Row buffer) {
        return buffer.getString(0);
    }
}
