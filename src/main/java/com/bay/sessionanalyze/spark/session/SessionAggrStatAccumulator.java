package com.bay.sessionanalyze.spark.session;

import com.bay.sessionanalyze.constant.Constants;
import com.bay.sessionanalyze.util.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * 使用自定义的数据格式,比如现在要用String格式,可以自定义model
 * 自定义类必须要可序列化,可以基于这种特殊的格式,实现复杂的分布式累加计算
 * 每个Task被分布在集群中的各个Executor中运行,可以根据需求,Task给Accumulator传入不同的值
 * 最后根据不同的值,做累加计算
 * <p>
 * Author by BayMin, Date on 2018/8/15.
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String> {
    /**
     * 主要是对数据做初始化
     * 在这个需求中,值返回一个值,就是在初始化中所有范围区间的数据都为0
     * 各个范围区间的统计数量的拼接,还是以字符串拼接的方式:key=value|key=value|...
     */
    @Override
    public String zero(String initialValue) {
        return Constants.SESSION_COUNT + "=0|" +
                Constants.TIME_PERIOD_1s_3s + "=0|" +
                Constants.TIME_PERIOD_4s_6s + "=0|" +
                Constants.TIME_PERIOD_7s_9s + "=0|" +
                Constants.TIME_PERIOD_10s_30s + "=0|" +
                Constants.TIME_PERIOD_30s_60s + "=0|" +
                Constants.TIME_PERIOD_1m_3m + "=0|" +
                Constants.TIME_PERIOD_3m_10m + "=0|" +
                Constants.TIME_PERIOD_10m_30m + "=0|" +
                Constants.TIME_PERIOD_30m + "=0|" +
                Constants.STEP_PERIOD_1_3 + "=0|" +
                Constants.STEP_PERIOD_4_6 + "=0|" +
                Constants.STEP_PERIOD_7_9 + "=0|" +
                Constants.STEP_PERIOD_10_30 + "=0|" +
                Constants.STEP_PERIOD_30_60 + "=0|" +
                Constants.STEP_PERIOD_60 + "=0|";
    }

    /**
     * 用来实现value1初始化的连接字符串
     * value1就是zero中的初始值
     * value2就是遍历Session的是否,判断某个Session对应的区间,然后用Constants.TIME_PERIOD_7s_9s找到相应的值
     * 实现的就是在value1中value2对应的value,进行累加1,最后再更新到初始连接字符串
     */
    @Override
    public String addAccumulator(String t1, String t2) {
        return add(t1, t2);
    }

    /**
     * 可以理解为和addAccumulator一样的逻辑
     */
    @Override
    public String addInPlace(String r1, String r2) {
        return add(r1, r2);
    }

    private String add(String v1, String v2) {
        // 验证v1为空的话，直接返回v2
        if (StringUtils.isEmpty(v1))
            return v2;
        // 如果v1不为空，从v1中提取v2对应的值，然后累加1
        String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
        if (oldValue != null) {
            // 将范围区间原有的值转换为int类型后累加1
            int newValue = Integer.valueOf(oldValue) + 1;
            // 将v1中v2对应的值，更新为新的累加后的值
            return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
        }
        return v1;
    }
}
