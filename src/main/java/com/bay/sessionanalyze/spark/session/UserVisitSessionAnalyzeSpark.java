package com.bay.sessionanalyze.spark.session;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bay.sessionanalyze.constant.Constants;
import com.bay.sessionanalyze.dao.*;
import com.bay.sessionanalyze.dao.factory.DAOFactory;
import com.bay.sessionanalyze.domain.*;
import com.bay.sessionanalyze.util.*;
import com.google.common.base.Optional;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.HashMap;
import java.util.Date;
import java.util.Iterator;


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
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("UserVisitSessionAnalyzeSpark");
        SparkUtils.setMaster(conf);
        // 创建集群入口类
        JavaSparkContext sc = new JavaSparkContext(conf); // Java中使用JavaSparkContext
        // SparkSQL的上下文对象
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc()); // 本地使用的是临时表模拟,集群使用Hive生产
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
        System.out.println(sessionId2DetailRDD.count());

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
        randomExtranctSession(sc, task.getTaskid(), filteredSessionId2AggrInfoRDD, sessionId2DetailRDD);

        /*
            计算top10热门品类
            1.获取通过筛选条件的Session访问过得所有品类
            2.计算出Session访问过得所有品类的点击,西单,支付次数,需要用到join
            3.实现自定义排序类
            4.将品类的点击,下单,支付次数封装到自定义排序key中
            5.使用sortByKey进行二次排序
            6.获取排序后的前10个品类:take(10)
            7.将top10热门品类即每个品类的点击下单支付次数写入数据库
         */
        List<Tuple2<CategorySortKey, String>> top10CategoryList = getTop10Category(task.getTaskid(), sessionId2DetailRDD);

        sc.stop();
    }

    /**
     * 计算top10热门品类
     */
    private static List<Tuple2<CategorySortKey, String>> getTop10Category(long taskid, JavaPairRDD<String, Row> sessionId2DetailRDD) {
        /*第一步:获取符合条件的Session访问的所有品类*/
        // 获取Session访问的所有品类id,(点击过,下单过,支付过)
        JavaPairRDD<Long, Long> categoryIdRDD = sessionId2DetailRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                Row row = stringRowTuple2._2; // SessionId对应的行为数据
                // 用于存储点击,下单,支付的品类信息
                List<Tuple2<Long, Long>> list = new ArrayList<>();
                // 添加点击品类信息
                Long clickCategoryId = row.getLong(6);
                if (clickCategoryId != null) {
                    list.add(new Tuple2<>(clickCategoryId, clickCategoryId));
                }
                // 添加下单信息
                String orderCategoryIds = row.getString(8);
                if (orderCategoryIds != null) {
                    String[] orderCategoryIdsSplied = orderCategoryIds.split(",");
                    for (String orderCategoryId : orderCategoryIdsSplied) {
                        Long longOrderCategoryId = Long.valueOf(orderCategoryId);
                        list.add(new Tuple2<>(longOrderCategoryId, longOrderCategoryId));
                    }
                }
                // 添加支付信息
                String payCategoryIds = row.getString(10);
                if (payCategoryIds != null) {
                    String[] payCategoryIdsSplied = payCategoryIds.split(",");
                    for (String payCategoryId : payCategoryIdsSplied) {
                        Long longPayCategoryId = Long.valueOf(payCategoryId);
                        list.add(new Tuple2<Long, Long>(longPayCategoryId, longPayCategoryId));
                    }
                }
                return list;
            }
        });
        // Session访问过得所有品类中,可能有重复的categoryId.需要去重
        categoryIdRDD = categoryIdRDD.distinct();
        /*第二步:计算品类点击,下单,支付次数*/
        // 计算各品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionId2DetailRDD);
        // 计算各品类的下单次数
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionId2DetailRDD);
        // 计算各品类的支付次数
        JavaPairRDD<Long, Long> payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionId2DetailRDD);
        /*第三步:join个品类和它的点击,下单,支付次数
          categoryIdRDD数据里面,包含了所有符合条件的过滤掉重复品类的Session
          在第二步中分别计算了点击下单支付次数,可能不是包含所有品类的
          比如:有点品类只是点击过,但没有下单,类似的这种情况很多
          所以在这里如果要做join,就不能用join,需要用lefOuterJoin
        */
        JavaPairRDD<Long, String> categoryId2CountRDD = joinCategoryAndDetail(categoryIdRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD, payCategoryId2CountRDD);
        /*第四步:实现自定义排序*/

        /*第五步:将数据映射为<CategorySortKey,countInfo>,再进行二次排序*/
        JavaPairRDD<CategorySortKey, String> sortKeyCountRDD = categoryId2CountRDD.mapToPair(new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {
            @Override
            public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> longStringTuple2) throws Exception {
                String countInfo = longStringTuple2._2;
                long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
                long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
                long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));
                // 创建自定义排序示例
                CategorySortKey sortKey = new CategorySortKey(clickCount, orderCount, payCount);
                return new Tuple2<>(sortKey, countInfo);
            }
        });
        // 进行降序排序
        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKeyCountRDD.sortByKey(false);
        /*第六步:取出前十个并持久化到数据库*/
        List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD.take(10);
        ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
        for (Tuple2<CategorySortKey, String> tuple2 : top10CategoryList) {
            String countInfo = tuple2._2;
            long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));
            Top10Category top10Category = new Top10Category();
            top10Category.setTaskid(taskid);
            top10Category.setCategoryid(categoryId);
            top10Category.setClickCount(clickCount);
            top10Category.setOrderCount(orderCount);
            top10Category.setPayCount(payCount);
            top10CategoryDAO.insert(top10Category);
        }
        return top10CategoryList;
    }

    /**
     * 链接品类RDD和数据RDD
     */
    private static JavaPairRDD<Long, String> joinCategoryAndDetail(JavaPairRDD<Long, Long> categoryIdRDD, JavaPairRDD<Long, Long> clickCategoryId2CountRDD, JavaPairRDD<Long, Long> orderCategoryId2CountRDD, JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
        // 注意:如果用leftOuterJoin,就可能出现右边RDD中join过来的值为空的情况
        // 所有tuple中的第二个值用Optional<Long>类型,代表可能有值,也可能没有值
        JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD = categoryIdRDD.leftOuterJoin(clickCategoryId2CountRDD);
        // 把数据生成格式为(categoryId,"categoryId=品类|clickCount=点击次数")
        JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> longTuple2Tuple2) throws Exception {
                long categoryId = longTuple2Tuple2._1;
                Optional<Long> longOptional = longTuple2Tuple2._2._2;
                long clickCount = 0L;
                if (longOptional.isPresent()) {
                    clickCount = longOptional.get();
                }
                String value = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCount;

                return new Tuple2<>(categoryId, value);
            }
        });
        // 再次与下单次数进行join
        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> longTuple2Tuple2) throws Exception {
                long categoryId = longTuple2Tuple2._1;
                String value = longTuple2Tuple2._2._1;
                Optional<Long> longOptional = longTuple2Tuple2._2._2;
                long orderCount = 0L;
                if (longOptional.isPresent()) {
                    orderCount = longOptional.get();
                }
                value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;
                return new Tuple2<>(categoryId, value);
            }
        });
        // 再与支付次数进行leftOuterJoin
        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> longTuple2Tuple2) throws Exception {
                long categoryId = longTuple2Tuple2._1;
                String value = longTuple2Tuple2._2._1;
                Optional<Long> longOptional = longTuple2Tuple2._2._2;
                long payCount = 0;
                if (longOptional.isPresent()) {
                    payCount = longOptional.get();
                }
                value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;
                return new Tuple2<>(categoryId, value);
            }
        });
        return tmpMapRDD;
    }

    /**
     * 计算各品类的支付次数
     */
    private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2DetailRDD) {
        // 过滤支付字段值为空的数据
        JavaPairRDD<String, Row> payActionRDD = sessionId2DetailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v1) throws Exception {
                Row row = v1._2;
                return row.getString(10) != null ? true : false;
            }
        });
        // 生成元组便于聚合
        JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                Row row = stringRowTuple2._2;
                String payCategoryIds = row.getString(10);
                String[] payCategoryIdsSplited = payCategoryIds.split(",");
                // 用于存储切分后的数据(payCategoryId,1L)
                List<Tuple2<Long, Long>> list = new ArrayList<>();
                for (String payCategoryId : payCategoryIdsSplited) {
                    list.add(new Tuple2<>(Long.valueOf(payCategoryId), 1L));
                }
                return list;
            }
        });
        // 聚合
        JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        return payCategoryId2CountRDD;
    }

    /**
     * 计算各品类的下单次数
     */
    private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2DetailRDD) {
        // 过滤下单字段值为空的数据
        JavaPairRDD<String, Row> orderActionRDD = sessionId2DetailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v1) throws Exception {
                Row row = v1._2;
                return row.getString(8) != null ? true : false;
            }
        });
        // 生成元组便于聚合
        JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                Row row = stringRowTuple2._2;
                String orderCategoryIds = row.getString(8);
                String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                // 用于存储切分后的数据(orderCategoryId,1L)
                List<Tuple2<Long, Long>> list = new ArrayList<>();
                for (String orderCategoryId : orderCategoryIdsSplited) {
                    list.add(new Tuple2<>(Long.valueOf(orderCategoryId), 1L));
                }
                return list;
            }
        });
        // 聚合
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        return orderCategoryId2CountRDD;
    }

    /**
     * 计算各品类的点击次数
     */
    private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2DetailRDD) {
        // 把明细数据中的点击品类字段的空字段过滤掉
        JavaPairRDD<String, Row> clickActionRDD = sessionId2DetailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v1) throws Exception {
                Row row = v1._2;
                return row.get(6) != null ? true : false;
            }
        });
        // 将每一个点击品类后面跟一个1,生成元组 (clickCategoryId,1)为了做聚合
        JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                long clickCategoryId = stringRowTuple2._2.getLong(6);
                return new Tuple2<>(clickCategoryId, 1L);
            }
        });
        // 计算各个品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        return clickCategoryId2CountRDD;
    }

    /**
     * 按照时间比例随机收取Session
     */
    private static void randomExtranctSession(JavaSparkContext sc, final long taskid, JavaPairRDD<String, String> filteredSessionId2AggrInfoRDD, JavaPairRDD<String, Row> sessionId2DetailRDD) {
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
        // 将countMap<yyyy-MM-dd_HH,count>转换为<yyyy-MM-dd,<HH,count>>放到Map中
        Map<String, Map<String, Long>> dateHourCountMap = new HashMap<>();
        // 调用循环把数据放到dateHourCountMap中
        for (Map.Entry<String, Object> countEntry : countMap.entrySet()) {
            // 取出日期和时间
            String dateHour = countEntry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];
            // 取出每小时的count数
            Long count = Long.valueOf(String.valueOf(countEntry.getValue()));
            // 用来存储<hour,count>的Map
            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if (hourCountMap == null) {
                hourCountMap = new HashMap<>();
                dateHourCountMap.put(date, hourCountMap);
            }
            hourCountMap.put(hour, count);
        }
        // 实现按时间比例抽取算法
        // 现在要从数据中抽取100个Session,先按照天数进行评分
        int extractNumber = 100 / dateHourCountMap.size();
        // Map<date,Map<hour,List(2,13,5,4,7...)>> List中是随机抽取的索引
        Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<>();
        Random random = new Random();
        for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue(); // 小时数对应的count数
            // 计算出当天的Session总数
            Long sessionCount = 0L;
            for (long hourCount : hourCountMap.values()) {
                sessionCount += hourCount;
            }
            // 把一天的Session数量put到dateHourExtractMap
            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<>();
                dateHourExtractMap.put(date, hourExtractMap);
            }
            // 遍历每个小时,获取每个小时的Session数量
            for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey(); // 小时
                long count = hourCountEntry.getValue(); // 小时对应的count数
                // 计算每个小时Session数量占当天Session的数量占比,乘以要抽取的数量
                // 最后计算出当前小时需要抽取的Session数量
                int hourExtractNumber = (int) (((double) count / (double) sessionCount) * extractNumber);
                // 当前要抽取的Session数量有可能大于每小时Session数量
                // 让当前小时需要抽取的Session数量直接等于每小时Session数量
                if (hourExtractNumber > count) {
                    hourExtractNumber = (int) count;
                }
                // 获取当前小时的存放随机数的List,如果没有就创建一个
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if (extractIndexList == null) {
                    extractIndexList = new ArrayList<>();
                    hourExtractMap.put(hour, extractIndexList);
                }
                // 生成上面计算出来的随机数,用while去判断生成随机数是否重复
                for (int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt((int) count);
                    while (extractIndexList.contains(extractIndex)) {
                        // 如果随机数重复,则重新生成随机数
                        extractIndex = random.nextInt((int) count);
                    }
                    extractIndexList.add(extractIndex);
                }
            }
        }
        // 把dateHourExtractMap封装到fastUtilDateHourExtractMap中(占用内存小,减少网络IO)
        // fastUtil可以封装Map,List,Set等,相比较Java中自带的占用内存更小,在分布式计算中减少网络IO量,传输速度快,占用的网络带宽更小
        Map<String, Map<String, IntList>> fastUtilDateHourExtractMap = new HashMap<>();
        for (Map.Entry<String, Map<String, List<Integer>>> dateHourExtractEntry : dateHourExtractMap.entrySet()) {
            // 日期
            String date = dateHourExtractEntry.getKey();
            // <hour,extract>
            Map<String, List<Integer>> hourExtractMap = dateHourExtractEntry.getValue();
            // 用于存放<hour,extract>
            Map<String, IntList> fastUtilHourExtractMap = new HashMap<>();
            for (Map.Entry<String, List<Integer>> hourExtractEntry : hourExtractMap.entrySet()) {
                // 小时
                String hour = hourExtractEntry.getKey();
                // <extract>
                List<Integer> extractList = hourExtractEntry.getValue();
                // 封装
                IntList fastUtilExtractList = new IntArrayList();
                for (int i = 0; i < extractList.size(); i++) {
                    fastUtilExtractList.add(extractList.get(i));
                }
                fastUtilHourExtractMap.put(hour, fastUtilExtractList);
            }
            fastUtilDateHourExtractMap.put(date, fastUtilHourExtractMap);
        }
        // 集群执行task时,有可能多个Executor会远程回去上面的Map值
        // 这样会产生大量的网络IO,此时最好用广播变量到每一个参与计算的Executor
        final Broadcast<Map<String, Map<String, IntList>>> dateHourExtractMapBroadcast = sc.broadcast(fastUtilDateHourExtractMap);
        /*第三步:遍历每天每小时的Session,根据随机抽取的索引开始抽取*/
        // 需要获取到<dateHour,<session,aggrInfo>>
        JavaPairRDD<String, Iterable<String>> time2SessionRDD = time2SessionIdRDD.groupByKey();
        // 遍历每天每小时的Session
        // 如果发现某个Session正好在指定的这天这个小时的随机索引上
        // 将该Session写入到数据库
        // 然后将抽取出来的Session返回,生成新的JavaRDD<String>
        // 用抽取出来的SessionId,去join他们的访问明细,在写入数据库表
        JavaPairRDD<String, String> extractSessionIdsRDD = time2SessionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
            @Override
            public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
                // 用来存储<sessionId>
                List<Tuple2<String, String>> extractSessionIds = new ArrayList<>();
                String dateHour = stringIterableTuple2._1;
                String date = dateHour.split("_")[0];
                String hour = dateHour.split("_")[1];
                Iterator<String> iterator = stringIterableTuple2._2.iterator();
                // 调用广播过来的值
                Map<String, Map<String, IntList>> dateHourExtractMap = dateHourExtractMapBroadcast.getValue();
                // 获取抽取索引List
                IntList extractIndexList = dateHourExtractMap.get(date).get(hour);
                ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();
                int index = 0;
                while (iterator.hasNext()) {
                    String sessionAggrInfo = iterator.next();
                    if (extractIndexList.contains(index)) {
                        String sessionId = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
                        // 将数据存入数据库
                        SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                        sessionRandomExtract.setTaskid(taskid);
                        sessionRandomExtract.setSessionid(sessionId);
                        sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));
                        sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                        sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
                        sessionRandomExtractDAO.insert(sessionRandomExtract);
                        // 将SessionId放入List
                        extractSessionIds.add(new Tuple2<String, String>(sessionId, sessionId));
                    }
                    index++;
                }
                return extractSessionIds;
            }
        });
        /*第四步:获取抽取出来的Session对应的明细并存入数据库*/
        // 把明细join出来
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = extractSessionIdsRDD.join(sessionId2DetailRDD);
        extractSessionDetailRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Tuple2<String, Row>>> tuple2Iterator) throws Exception {
                // 用来存储明细数据的List
                List<SessionDetail> sessionDetails = new ArrayList<>();
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, Tuple2<String, Row>> tuple = tuple2Iterator.next();
                    Row row = tuple._2._2;
                    SessionDetail sessionDetail = new SessionDetail();
                    sessionDetail.setTaskid(taskid);
                    sessionDetail.setUserid(row.getLong(1));
                    sessionDetail.setSessionid(row.getString(2));
                    sessionDetail.setPageid(row.getLong(3));
                    sessionDetail.setActionTime(row.getString(4));
                    sessionDetail.setSearchKeyword(row.getString(5));
                    sessionDetail.setClickCategoryId(row.getLong(6));
                    sessionDetail.setClickProductId(row.getLong(7));
                    sessionDetail.setOrderCategoryIds(row.getString(8));
                    sessionDetail.setOrderProductIds(row.getString(9));
                    sessionDetail.setPayCategoryIds(row.getString(10));
                    sessionDetail.setPayProductIds(row.getString(11));
                    sessionDetails.add(sessionDetail);
                }
                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insertBatch(sessionDetails);
            }
        });
    }

    /**
     * 将数据插入到表中
     */
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