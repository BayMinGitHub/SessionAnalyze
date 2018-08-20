package com.bay.sessionanalyze.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.bay.sessionanalyze.constant.Constants;
import com.bay.sessionanalyze.dao.ITaskDAO;
import com.bay.sessionanalyze.dao.factory.DAOFactory;
import com.bay.sessionanalyze.domain.Task;
import com.bay.sessionanalyze.util.ParamUtils;
import com.bay.sessionanalyze.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

/**
 * 地区商品Top3
 * 1.获取按照使用者指定日期范围内的点击行为数据
 * 2.获取城市信息(区域)
 * 3.按照区域来分组,并排序
 * 4.得到区域Top3热门商品
 * 5.持久化到数据库中
 * <p>
 * Author by BayMin, Date on 2018/8/20.
 */
public class AreaTop3ProductSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PRODUCT);
        SparkUtils.setMaster(conf);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
        // 注册自定义函数UDF

        // 获取数据
        SparkUtils.mockData(sc, sqlContext);
        // 获取taskId,查询到相应的任务信息
        Long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskId);
        // 将task信息封装为JSONObject对象
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        // 获取使用者指定的开始时间和结束时间
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
        // 查询用户指定日期范围内的点击行为数据:<city_id,action>
        JavaPairRDD<Long, Row> cityId2ClickActionRDD = getCityId2ClickActionRDD(sqlContext, startDate, endDate);

    }

    /**
     * 查询用户指定日期范围内的点击行为数据
     */
    private static JavaPairRDD<Long, Row> getCityId2ClickActionRDD(SQLContext sqlContext, String startDate, String endDate) {
        /*
         * 从user_visit_action表中查询用户访问行为数据
         * 第一个限定:click_product_id限定为不为空的访问行为,这个字段的值就代表点击商品行为
         * 第二个限定:在使用者指定日期范围内的数据
         */
        String sql = "select city_id,click_product_id as product_id from user_visit_action " +
                "where click_product_id is not null " +
                "and session_date >= '" + startDate + "' " +
                "and session_date <= '" + endDate + "' ";
        DataFrame clickActionDF = sqlContext.sql(sql);
        // 把生成的DataFrame转化成RDD
        JavaRDD<Row> clickActionRDD = clickActionDF.javaRDD();
        JavaPairRDD<Long, Row> cityId2ClickActionRDD = clickActionRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getLong(0), row);
            }
        });
        return cityId2ClickActionRDD;
    }
}
