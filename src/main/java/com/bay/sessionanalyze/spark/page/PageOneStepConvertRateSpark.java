package com.bay.sessionanalyze.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.bay.sessionanalyze.constant.Constants;
import com.bay.sessionanalyze.dao.IPageSplitConvertRateDAO;
import com.bay.sessionanalyze.dao.ITaskDAO;
import com.bay.sessionanalyze.dao.factory.DAOFactory;
import com.bay.sessionanalyze.domain.PageSplitConvertRate;
import com.bay.sessionanalyze.domain.Task;
import com.bay.sessionanalyze.util.DateUtils;
import com.bay.sessionanalyze.util.NumberUtils;
import com.bay.sessionanalyze.util.ParamUtils;
import com.bay.sessionanalyze.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;

/**
 * 页面单跳转化率
 * 1.首先获取按使用者传的页面流过滤出来的数据
 * 2.生成页面切片
 * 2.生成页面流
 * 4.根据页面切片生成单挑转化率
 * 5.数据的存储
 * <p>
 * Author by BayMin, Date on 2018/8/20.
 */
public class PageOneStepConvertRateSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PAGE);
        SparkUtils.setMaster(conf);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
        // 本地测试中获取数据
        SparkUtils.mockData(sc, sqlContext);
        // 查询任务,获取任务信息
        Long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskId);
        if (task == null)
            throw new RuntimeException(new Date() + "无法获取到相关任务信息");
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        // 查询指定日期范围内的用户访问的行为数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);
        // 对用户访问行为数据做映射,将数据映射为<session,访问行为数据>格式的Session粒度的数据
        // 因为数据访问页面切片的生成,是基于每个Session的访问数据生成的
        // 如果脱离了Session,生成的页面切面是没有意义的
        JavaPairRDD<String, Row> sessionId2ActionRDD = getSessionId2Action(actionRDD);
        // 缓存
        sessionId2ActionRDD = sessionId2ActionRDD.cache();
        // 因为要拿到多个Session对应的行为数据,才能生成页面切片
        JavaPairRDD<String, Iterable<Row>> groupedSessionId2ActionRDD = sessionId2ActionRDD.groupByKey();
        // 这个需求中最关键的一步,就是每个Session的单挑页面切片的生成和页面流的匹配算法
        // <split,1>
        JavaPairRDD<String, Integer> pageSplitRDD = generateAndMatchPageSplit(sc, groupedSessionId2ActionRDD, taskParam);
        // 获取切片的访问量
        Map<String, Object> pageSplitPVMap = pageSplitRDD.countByKey();
        // 获取起始页面的访问量
        long startPagePV = getStartPagePV(taskParam, groupedSessionId2ActionRDD);
        // 计算目标页面的各个页面切片的转化率
        Map<String, Double> convertRateMap = computePageSplitConvertRate(taskParam, pageSplitPVMap, startPagePV);
        // 结果持久化
        persistConvertRate(taskId, convertRateMap);

        sc.stop();
    }

    /**
     * 把页面切片持久化到数据库
     */
    private static void persistConvertRate(long taskId, Map<String, Double> convertRateMap) {
        // 声明一个buffer,用于存储页面流对应的切片和转化率
        StringBuffer buffer = new StringBuffer();
        for (Map.Entry<String, Double> convertRageEntry : convertRateMap.entrySet()) {
            // 获取切片
            String pageSplit = convertRageEntry.getKey();
            double convertRage = convertRageEntry.getValue();
            // 拼接
            buffer.append(pageSplit + "=" + convertRage + "|");
        }
        // 获取品结构的切片和转化率
        String convertRate = buffer.toString();
        if (convertRate.endsWith("|"))
            convertRate = convertRate.substring(0, convertRate.length() - 1);
        PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
        pageSplitConvertRate.setTaskid(taskId);
        pageSplitConvertRate.setConvertRate(convertRate);
        IPageSplitConvertRateDAO pageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO();
        pageSplitConvertRateDAO.insert(pageSplitConvertRate);
    }

    /**
     * 计算页面切片的转换率
     */
    private static Map<String, Double> computePageSplitConvertRate(JSONObject taskParam, Map<String, Object> pageSplitPVMap, long startPagePV) {
        // 用于存储页面切片对应的转化率
        // Map<String,Double>:key=各个页面切片,value=页面切片对应的转化率
        Map<String, Double> convertRateMap = new HashMap<>();
        // 获取页面流
        String[] targetPages = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",");
        // 初始化上一个页面切片的访问量
        long lastPageSplitPV = 0L;
        /*
         * 求转化率:
         * 如果页面流为:1,3,5,6
         * 第一个页面切片:1_3
         * 第一个页面的转化率:3的PV/1的PV
         * */
        // 通过for循环,获取目标页面流中的各个页面切片和访问量
        for (int i = 1; i < targetPages.length; i++) {
            // 获取页面切片
            String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
            // 获取每个页面切片对应的访问量
            long targetPageSplitPV = Long.valueOf(String.valueOf(pageSplitPVMap.get(targetPageSplit)));
            // 初始化转化率
            Double convertRate = 0.0;
            // 生成转化率
            if (i == 1) {
                convertRate = NumberUtils.formatDouble((double) targetPageSplitPV / (double) startPagePV, 2);
            } else {
                convertRate = NumberUtils.formatDouble((double) targetPageSplitPV / (double) lastPageSplitPV, 2);
            }
            lastPageSplitPV = targetPageSplitPV;
            convertRateMap.put(targetPageSplit, convertRate);
        }
        return convertRateMap;
    }

    /**
     * 获取起始页面的访问量
     */
    private static long getStartPagePV(JSONObject taskParam, JavaPairRDD<String, Iterable<Row>> groupedSessionId2ActionRDD) {
        // 拿到使用者提供的页面流
        String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        // 从页面流中获取起始页面Id
        final Long StartPageId = Long.valueOf(targetPageFlow.split(",")[0]);
        JavaRDD<Long> startPageRDD = groupedSessionId2ActionRDD.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Row>>, Long>() {
            @Override
            public Iterable<Long> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
                // 用于存储每个Session访问的起始页面Id
                List<Long> list = new ArrayList<>();
                // 获取对应的行为数据
                Iterator<Row> iterator = stringIterableTuple2._2.iterator();
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    long pageId = row.getLong(3);
                    if (pageId == StartPageId) {
                        list.add(pageId);
                    }
                }
                return list;
            }
        });
        return startPageRDD.count();
    }

    /**
     * 页面切片的生成和页面流匹配算法
     */
    private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(JavaSparkContext sc, JavaPairRDD<String, Iterable<Row>> groupedSessionId2ActionRDD, JSONObject taskParam) {
        // 首先获取页面流
        final String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        // 把目标页面流广播到对应的Executor
        final Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetPageFlow);
        // 实现页面流匹配算法
        return groupedSessionId2ActionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Integer>() {
            @Override
            public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
                // 用于存储切片,格式为<split,1>
                List<Tuple2<String, Integer>> list = new ArrayList<>();
                // 获取当期Session对应的一组行为数据
                Iterator<Row> iterator = stringIterableTuple2._2.iterator();
                // 获取使用者指定的页面流
                String[] targetPages = targetPageFlowBroadcast.value().split(",");
                /*
                 * 代码运行到这里,session的访问数据已经拿到了
                 * 在默认情况下并没有排序,
                 * 在实现转换率的时候需要把数据按照时间排序
                 * */
                // 把访问行为数据放到list里,便于排序
                List<Row> rows = new ArrayList<>();
                while (iterator.hasNext()) {
                    rows.add(iterator.next());
                }
                // 按照时间排序,可以用自定义的排序的方式,也可以用匿名内部类的方式排序
                Collections.sort(rows, new Comparator<Row>() {
                    @Override
                    public int compare(Row o1, Row o2) {
                        String actionTime1 = o1.getString(4);
                        String actionTime2 = o2.getString(4);
                        Date date1 = DateUtils.parseTime(actionTime1);
                        Date date2 = DateUtils.parseTime(actionTime2);
                        return (int) (date1.getTime() - date2.getTime());
                    }
                });
                /* 生成页面切片,并和页面流进行匹配 */
                // 定义一个上一个页面的id
                Long lastPageId = null;
                // 注意:现在拿到的rows里的数据是其中一个sessionId对应的所有行为数据
                for (Row row : rows) {
                    long pageId = row.getLong(3);
                    if (lastPageId == null) {
                        lastPageId = pageId;
                        continue;
                    }
                    /*
                     * 生成一个页面切片
                     * 比如该用户请求的页面是:1,3,4,7
                     * 上次访问的页面id:lastPageId=1
                     * 这次请求的页面是:3
                     * 那么生成的页面切片为:1_3
                     * */
                    String pageSplit = lastPageId + "_" + pageId;
                    // 对这个页面切片判断一下,是否在使用者指定的页面流中
                    for (int i = 1; i < targetPages.length; i++) {
                        /*
                         * 比如说:使用者指定的页面流是:1,2,5,6
                         * 遍历的时候,从索引1开始,也就是从第二个页面开始
                         * 这样第一个页面切片就是1_2
                         * */
                        String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
                        if (pageSplit.equals(targetPageSplit)) {
                            list.add(new Tuple2<>(pageSplit, 1));
                            break;
                        }
                    }
                    lastPageId = pageId;
                }
                return list;
            }
        });
    }

    /**
     * 将SessionId与Action做映射
     */
    private static JavaPairRDD<String, Row> getSessionId2Action(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(2), row);
            }
        });
    }
}
