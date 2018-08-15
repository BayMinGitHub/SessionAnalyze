package com.bay.sessionanalyze.spark.session;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bay.sessionanalyze.constant.Constants;
import com.bay.sessionanalyze.dao.ISessionAggrStatDAO;
import com.bay.sessionanalyze.dao.ITaskDAO;
import com.bay.sessionanalyze.dao.factory.DAOFactory;
import com.bay.sessionanalyze.domain.SessionAggrStat;
import com.bay.sessionanalyze.domain.Task;
import com.bay.sessionanalyze.util.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

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
        // Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("UserVisitSessionAnalyzeSpark");
        SparkUtils.setMaster(conf);
        // 创建集群入口类
        JavaSparkContext sc = new JavaSparkContext(conf); // Java中使用JavaSparkContext
        // SparkSQL的上下文对象
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());// 本地使用的是MySQL测试,集群使用Hive生产
        // 设置检查点
        // sc.checkpointFile("hdfs://hadoop010:9000/sessionanalyze");
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
        // 生成Session粒度的基础数据,得到的数据格式为:<sessionId,actionRDD>
        JavaPairRDD<String, Row> sessionId2ActionRDD = getSessionId2ActionRDD(actionRDD);
        // 对于以后经常用到的数据,最好缓存起来,这样便于以后快速的获取该数据
        sessionId2ActionRDD = sessionId2ActionRDD.cache();
        // 对行为数据进行聚合
        // 1.将行为数据按照SessionId进行分组
        // 2.行为数据RDD需要把用户信息获取到,此时需要用到join,这样就得到Session粒度的明细数据
        //   明细数据包含了Session对应的用户基本信息
        //   生成的格式为:<sessionId,(sessionId,searchKeywords,clickCategoryIds,age,professional,city,sex,...)>
        JavaPairRDD<String, String> sessionId2AggrInfoRDD = aggrgateBySession(sc, sqlContext, sessionId2ActionRDD);
        // 实现Accumulator累加器对数据字段的值进行累加
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());
        // 以Session粒度的数据进行聚合,按照使用者指定的筛选条件进行过滤
        JavaPairRDD<String, String> filteredSessionId2AggrInfoRDD = filteredSessionAndAggrStat(sessionId2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);
        // 缓存过滤后的数据
        filteredSessionId2AggrInfoRDD = filteredSessionId2AggrInfoRDD.persist(StorageLevel.MEMORY_AND_DISK());
        // 生成一个公共的RDD,通过筛选条件过滤出来的Session得到访问明细
        JavaPairRDD<String, Row> sessionId2DetailRDD = getSessionId2DetailRDD(filteredSessionId2AggrInfoRDD, sessionId2ActionRDD);
        // 缓存
        sessionId2DetailRDD = sessionId2DetailRDD.cache();
        // 如果将上一个聚合的统计结果写入数据库
        // 就必须给一个action算子进行触发后,才能真正执行任务,从Accumulator中获取数据
        System.out.println(1 + sessionId2DetailRDD.count());
        // 计算出各个范围的Session占比,并写入数据库
        calcalateAndPersistAggrStat(sessionAggrStatAccumulator.value(), taskId);
        /*
            按照时间比例随机抽取Session
            1.首先计算每个小时的Session数量
            2.计算出每个小时的Session数据量在一天的比例
              比如: 要取出100条Session数据
              表达式: 当前小时抽取的Session数量 = (每小时Session的数据量/Session的总量)*100
            3.按照比例进行随机抽取Session
         */
        // randomExtranctSession(sc, task.getTaskid(), filteredSessionId2AggrInfoRDD, sessionId2DetailRDD);
        sc.stop();
    }

    /**
     * 按照时间比例随机收取Session
     */
    private static void randomExtranctSession(JavaSparkContext sc, long taskid, JavaPairRDD<String, String> filteredSessionId2AggrInfoRDD, JavaPairRDD<String, Row> sessionId2DetailRDD) {
        /*第一步:计算出每个小时的Session数量*/
        // 首先需要将数据调整为:<date_hour,data>
        JavaPairRDD<String, String> time2SessionIdRDD = filteredSessionId2AggrInfoRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                // 获取聚合数据
                String aggrInfo = stringStringTuple2._2;
                // 在从聚合数据中拿到startTime
                String startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME);
                // 获取日期和时间(date_hour)
                String dateHour = DateUtils.getDateHour(startTime);
                return new Tuple2<String, String>(dateHour, aggrInfo);
            }
        });
        // 需要得到每天每小时的Session数量,然后计算出每天每小时Session抽取索引,遍历每天每小时的Session
        // 首先抽取出Session聚合数据,写入数据库表
        // time2SessionIdRDD的数据,是每天的某个小时的Session聚合数据
        // 计算每天每小时的Session数量
        Map<String, Object> countMap = time2SessionIdRDD.countByKey();
        /*第二步:使用时间比例随机抽取算法,计算出每天每小时抽取的Session索引*/
    }

    private static void calcalateAndPersistAggrStat(String value, Long taskId) {
        // 首先从Accumulator统计的字符串结果中获取各个值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));
        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围占比
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble((double) visit_length_1s_3s / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble((double) visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble((double) visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble((double) visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble((double) visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble((double) visit_length_1m_3m / (double) session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble((double) visit_length_3m_10m / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble((double) visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble((double) visit_length_30m / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble((double) step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble((double) step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble((double) step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble((double) step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble((double) step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble((double) step_length_60 / (double) session_count, 2);

        // 将统计结果封装到Domain对象里
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskId);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 结果存储
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }

    /**
     * 获取通过筛选条件的Session的访问明细数据
     */
    private static JavaPairRDD<String, Row> getSessionId2DetailRDD(JavaPairRDD<String, String> filteredSessionId2AggrInfoRDD, JavaPairRDD<String, Row> sessionId2ActionRDD) {
        // 得到SessionId对应的按照使用者条件过滤后的明细数据
        JavaPairRDD<String, Row> sessionId2DetailRDD = filteredSessionId2AggrInfoRDD.join(sessionId2ActionRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> stringTuple2Tuple2) throws Exception {
                return new Tuple2<String, Row>(stringTuple2Tuple2._1, stringTuple2Tuple2._2._2);
            }
        });
        return sessionId2DetailRDD;
    }

    /**
     * 按照使用者条件过滤Session粒度的数据,并进行聚合
     */
    private static JavaPairRDD<String, String> filteredSessionAndAggrStat(JavaPairRDD<String, String> sessionId2AggrInfoRDD, JSONObject taskParam, final Accumulator<String> sessionAggrStatAccumulator) {
        // 先把所有筛选条件提取出来并拼接为一条字符串
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String catgorys = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);
        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "") +
                (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "") +
                (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "") +
                (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "") +
                (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "") +
                (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "") +
                (catgorys != null ? Constants.PARAM_CATEGORY_IDS + "=" + catgorys + "|" : "");
        // 把_parameter的值的最后一个"|"截取掉
        if (_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }
        final String parameter = _parameter;
        // 根据筛选条件进行过滤
        JavaPairRDD<String, String> filteredSessionAggrInfoRDD = sessionId2AggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                // 从元组中获取基础数据
                String aggrInfo = v1._2;
                /*依次按照筛选条件进行过滤*/
                // 年龄
                if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE))
                    return false;
                // 职业
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS))
                    return false;
                // 城市
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES))
                    return false;
                // 性别
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX))
                    return false;
                // 关键字
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS))
                    return false;
                // 点击品类
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS))
                    return false;
                /*
                 代码执行到这里,说明该Session通过了用户指定的筛选条件
                 接下来要多Session的访问时长和访问步长进行统计
                 */
                // 根据Session对应的时长和步长的时间范围进行累加操作
                sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
                // 计算出Session的访问时长和访问步长的范围,并进行累加
                long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
                // 计算访问时长范围
                calculateVisitLength(visitLength);
                // 计算访问步长范围
                calculateStepLength(stepLength);
                return true;
            }

            /*计算访问时长范围*/
            private void calculateVisitLength(long visitLength) {
                if (visitLength >= 1 && visitLength <= 3)
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                else if (visitLength >= 4 && visitLength <= 6)
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                else if (visitLength >= 7 && visitLength <= 9)
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                else if (visitLength >= 10 && visitLength < 30)
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                else if (visitLength >= 30 && visitLength < 60)
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                else if (visitLength >= 60 && visitLength < 180)
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                else if (visitLength >= 180 && visitLength < 600)
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                else if (visitLength >= 600 && visitLength < 1800)
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                else if (visitLength >= 1800)
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
            }

            /*计算访问步长范围*/
            private void calculateStepLength(long stepLength) {
                if (stepLength >= 1 && stepLength <= 3)
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                else if (stepLength >= 4 && stepLength <= 6)
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                else if (stepLength >= 7 && stepLength <= 9)
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                else if (stepLength >= 10 && stepLength < 30)
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                else if (stepLength >= 30 && stepLength < 60)
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                else if (stepLength >= 60)
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
            }
        });
        return filteredSessionAggrInfoRDD;
    }

    /**
     * 对行为数据按照Session粒度进行聚合
     */
    private static JavaPairRDD<String, String> aggrgateBySession(JavaSparkContext sc, SQLContext sqlContext, JavaPairRDD<String, Row> sessionId2ActionRDD) {
        // 对行为数据进行分组
        JavaPairRDD<String, Iterable<Row>> sessionId2ActionPairRDD = sessionId2ActionRDD.groupByKey();
        // 对每个Session分组进行聚合,将Session中所有的搜索关键字和点击品类都聚合起来
        // 格式:<userId,partAggrInfo(sessionId,searchKeywords,clickCategoryIds,visitLength,stepLength,startTime)>
        JavaPairRDD<Long, String> userId2PartAggrInfoRDD = sessionId2ActionPairRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
                String sessionId = stringIterableTuple2._1;
                Iterator<Row> iterator = stringIterableTuple2._2.iterator();
                // 用来存储搜索关键字和点击品类
                StringBuffer searchKeywordsBuffer = new StringBuffer();
                StringBuffer clickCategoryIdsBuffer = new StringBuffer();
                // 用来存储userId
                Long userId = null; // 包装类可以使用null
                // 用来存储起始时间和结束时间
                Date startTime = null;
                Date endTime = null;
                // 用来存储Session的访问步长
                int stepLength = 0;
                // 遍历Session中所有的行为数据
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    if (userId == null) {
                        userId = row.getLong(1);
                    }
                    // 获取每个访问行为的搜索关键字和点击品类,注意:
                    // 如果该行为是搜索行为,searchKeyword是有值的
                    // 但同时点击行为就没有值,任何的行为,不可能有两个字段都有值
                    String searchKeyword = row.getString(5);
                    String clickCategroyId = String.valueOf(row.getLong(6));
                    // 追加搜索关键字
                    if (!StringUtils.isEmpty(searchKeyword)) {
                        if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                            searchKeywordsBuffer.append(searchKeyword + ",");
                        }
                    }
                    // 追加点击品类
                    if (clickCategroyId != null) {
                        if (!clickCategoryIdsBuffer.toString().contains(clickCategroyId)) {
                            clickCategoryIdsBuffer.append(clickCategroyId + ",");
                        }
                    }
                    // 计算Session的开始时间和结束时间
                    Date actionTime = DateUtils.parseTime(row.getString(4));
                    if (startTime == null) {
                        startTime = actionTime;
                    }
                    if (endTime == null) {
                        endTime = actionTime;
                    }
                    if (actionTime.before(startTime)) {
                        startTime = actionTime;
                    }
                    if (actionTime.after(endTime)) {
                        endTime = actionTime;
                    }
                    // 计算访问步长
                    stepLength++;
                }
                // 截取字符串中两端的逗号,得到搜索关键字和点击品类
                String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
                // 计算访问时长,单位为秒
                long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
                // 聚合数据,数据以字符串拼接的方式:key=value|key=value|...
                String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
                        Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" +
                        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" +
                        Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
                        Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
                        Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime) + "|";
                return new Tuple2<Long, String>(userId, partAggrInfo);
            }
        });
        // 查询所有用户数据,构建成<userId,Row>格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
        JavaPairRDD<Long, Row> userId2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<Long, Row>(row.getLong(0), row);
            }
        });
        // 将Session粒度的聚合数据和用户信息进行join,构建成<userId,<sessionInfo,userInfo>>格式
        JavaPairRDD<Long, Tuple2<String, Row>> userId2FullInfoRDD = userId2PartAggrInfoRDD.join(userId2InfoRDD);
        // 对join后的数据进行重新拼接,返回格式为:<sessionId,fullAggrInfo>
        JavaPairRDD<String, String> sessionId2FullAggrInfoRDD = userId2FullInfoRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> longTuple2Tuple2) throws Exception {
                // 先获取SessionId对应的聚合数据
                String partAggrInfo = longTuple2Tuple2._2._1;
                // 获取用户信息
                Row userInfoRow = longTuple2Tuple2._2._2;
                // 获取SessionId
                String sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
                // 提取用户信息的年龄,职业,所在城市,性别
                int age = userInfoRow.getInt(3);
                String professional = userInfoRow.getString(4);
                String city = userInfoRow.getString(5);
                String sex = userInfoRow.getString(6);
                // 拼接
                String fullAggrInfo = partAggrInfo +
                        Constants.FIELD_AGE + "=" + age + "|" +
                        Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
                        Constants.FIELD_CITY + "=" + city + "|" +
                        Constants.FIELD_SEX + "=" + sex + "|";
                return new Tuple2<String, String>(sessionId, fullAggrInfo);
            }
        });
        return sessionId2FullAggrInfoRDD;
    }

    /**
     * 获取SessionId对应的行为数据,生成Session粒度的数据
     */
    private static JavaPairRDD<String, Row> getSessionId2ActionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            @Override
            public Iterable<Tuple2<String, Row>> call(Iterator<Row> rowIterator) throws Exception {
                // 用来封装基础数据
                List<Tuple2<String, Row>> list = new ArrayList<>();
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    list.add(new Tuple2<String, Row>(row.getString(2), row));
                }
                return list;
            }
        });
    }
}
