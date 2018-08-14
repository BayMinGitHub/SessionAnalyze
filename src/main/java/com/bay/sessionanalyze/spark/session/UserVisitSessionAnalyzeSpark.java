package com.bay.sessionanalyze.spark.session;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bay.sessionanalyze.constant.Constants;
import com.bay.sessionanalyze.dao.ITaskDAO;
import com.bay.sessionanalyze.dao.factory.DAOFactory;
import com.bay.sessionanalyze.domain.Task;
import com.bay.sessionanalyze.util.ParamUtils;
import com.bay.sessionanalyze.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Date;

/**
 * 获取用户访问Session数据进行分析
 * 1.接收使用者创建的任务信息,任务中的过滤条件:
 * 时间范围:起始时间--结束时间
 * 年龄范围
 * 性别
 * 职业
 * 所在城市
 * 用户搜索的关键字
 * 点击品类
 * 点击商品
 * <p>
 * 2.Spark作业时如何接收使用者创建的任务信息
 * (1).Shell脚本通知--调用Spark-Submit脚本
 * (2).从MySQL的Task表中根据指定的TaskId来获取指定的任务信息
 * <p>
 * 3.SPark作业开始数据分析
 * <p>
 * Author by BayMin, Date on 2018/8/14.
 */
public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {
        /*模板代码*/
        // 创建配置信息类
        SparkConf conf = new SparkConf().setAppName("UserVisitSessionAnalyzeSpark");
        SparkUtils.setMaster(conf);
        // 创建集群入口类
        JavaSparkContext sc = new JavaSparkContext(conf); // Java中使用JavaSparkContext
        // SparkSQL的上下文对象
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());// 本地使用的是MySQL测试,集群使用Hive生产
        // 设置检查点
        sc.checkpointFile("hdfs://hadoop010:9000/...");
        // 生成模拟数据(测试)
        SparkUtils.mockData(sc, sqlContext);
        // 创建获取任务信息的实例
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        // 获取指定的任务,需要拿到TaskId
        Long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
        // 根据TaskId获取任务信息
        Task task = taskDAO.findById(taskId);
        if (task == null) {
            throw new RuntimeException(new Date() + "亲,您给的任务ID并不能查询到相应的信息哦");
        }
        // 根据Task去task_param字段获取对应的任务信息
        // task_param字段里存的就是使用者提供的查询条件
        JSONObject taskParam = JSON.parseObject(task.getTaskParam());
        // 开始查询指定日期范围内的行为数据(点击,搜索,下单,支付)
        // 首先要从user_visit_action这张hive表中查询出指定日期范围内的行为数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);

        sc.stop();
    }
}
