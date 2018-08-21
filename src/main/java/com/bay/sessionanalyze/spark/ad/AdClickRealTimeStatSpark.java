package com.bay.sessionanalyze.spark.ad;

import com.bay.sessionanalyze.conf.ConfigurationManager;
import com.bay.sessionanalyze.constant.Constants;
import com.bay.sessionanalyze.dao.IAdBlacklistDAO;
import com.bay.sessionanalyze.dao.IAdUserClickCountDAO;
import com.bay.sessionanalyze.dao.factory.DAOFactory;
import com.bay.sessionanalyze.domain.AdBlacklist;
import com.bay.sessionanalyze.domain.AdUserClickCount;
import com.bay.sessionanalyze.util.DateUtils;
import com.bay.sessionanalyze.util.SparkUtils;
import com.google.common.base.Optional;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * 广告点击流量实时统计:
 * 1.实时的过滤每天对某个广告点击超过100次的黑名单,更新到数据库
 * 2.实时计算各批次(batch)中每天个用户对各广告的点击次数
 * 3.实时的将每天各用户对个广告点击次数写入数据库(实时更新)
 * 4.对每个batch RDD进行处理,实现动态加载黑名单
 * 5.使用批次累加操作,实时的计算出每天各省个城市各广告的点击量
 * 6.统计每天各省top3热门广告
 * 7.统计最近1小时内的广告点击趋势
 * <p>
 * Author by BayMin, Date on 2018/8/21.
 */
public class AdClickRealTimeStatSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_AD);
        SparkUtils.setMaster(conf);
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(5));
        // 设置检查点
        javaStreamingContext.checkpoint("hdfs://hadoop010:9000/cp-20180821-1");
        // 构建请求kafka的参数
        Map<String, String> kafkaParam = new HashMap<>();
        kafkaParam.put("metadata.broker.list", ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
        // 构建topic
        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicSplited = kafkaTopics.split(",");
        // 存储topic
        Set<String> topics = new HashSet<>();
        for (String kafkaTopic : kafkaTopicSplited) {
            topics.add(kafkaTopic);
        }
        // 以流的方式读取kafka数据
        JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(javaStreamingContext, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParam, topics);
        // 根据动态黑名单进行数据过滤
        JavaPairDStream<String, String> filterAdRealTimeLogDStream = filterByBlackList(adRealTimeLogDStream);
        /*
         * 动态生成黑名单
         * 1.计算出每个batch中的每天每个用户对每个广告的点击量,并存入数据库
         * 2.依据上面的数据,对每个batch中的数据按照date,userId,adId聚合的数据都要遍历以便
         *   查询对应的累计的点击次数,如果超过了100次,就认为是黑名单用户
         * 3.实现动态更新黑名单
         * */
        dynamicGenerateBlackList(filterAdRealTimeLogDStream);


    }

    /**
     * 动态生成黑名单
     */
    private static void dynamicGenerateBlackList(JavaPairDStream<String, String> filterAdRealTimeLogDStream) {
        // 构建原始数据,将数据构建的格式为:<yyyyMMdd_userId_adId,1L>
        JavaPairDStream<String, Long> dailyUserAdClickDStream = filterAdRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                // 获取实时数据
                String log = stringStringTuple2._2;
                String[] logSplited = log.split(" ");
                // 获取yyyyMMdd userId adId
                String timeStamp = logSplited[0];
                Date date = new Date(Long.valueOf(timeStamp));
                String dateKey = DateUtils.formatDateKey(date); // yyyyMMdd
                long userId = Long.valueOf(logSplited[3]);
                long adId = Long.valueOf(logSplited[4]);
                // 拼接
                String key = dateKey + "_" + userId + "_" + adId;
                return new Tuple2<>(key, 1L);
            }
        });
        // 针对处理后的数据格式,进行聚合操作,聚合后的结果为每个batch中每天每个用户对每个广告的点击量
        JavaPairDStream<String, Long> dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        // 代码运行到这里,获取到什么数据了?
        // dailyUserAdClickCountDStream: <yyyyMMdd_userId_adId,count>
        // 把每天个用户对每个广告的点击量存入数据库
        dailyUserAdClickCountDStream.foreach(new Function<JavaPairRDD<String, Long>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Long> v1) throws Exception {
                v1.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> tuple2Iterator) throws Exception {
                        // 对每个分区的数据获取一次数据连接
                        // 每次都是从连接池中获取,而不是每次都创建
                        List<AdUserClickCount> adUserClickCounts = new ArrayList<>();
                        while (tuple2Iterator.hasNext()) {
                            Tuple2<String, Long> tuple = tuple2Iterator.next();
                            String[] keySplited = tuple._1.split("_");
                            // 获取date,userId,adId,clickCount
                            String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
                            long userId = Long.valueOf(keySplited[1]);
                            long adId = Long.valueOf(keySplited[2]);
                            long clickCount = tuple._2;
                            AdUserClickCount adUserClickCount = new AdUserClickCount();
                            adUserClickCount.setDate(date);
                            adUserClickCount.setUserid(userId);
                            adUserClickCount.setAdid(adId);
                            adUserClickCount.setClickCount(clickCount);
                            adUserClickCounts.add(adUserClickCount);
                        }
                        IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                        adUserClickCountDAO.updateBatch(adUserClickCounts);
                    }
                });
                return null;
            }
        });
        /*
         * 到这里,已经有了累计的每天个用户个广告的点击
         * 接下来遍历每个batch中所有的记录
         * 对每天记录都去查询一下这一天每个用户对这个广告的累计点击量
         * 判断如果某个用户某天对某广告的点击量大于等于100次
         * 就判断该用户为黑名单用户,把该用户更新到黑名单用户表中ad_blacklist中
         * */
    }

    /**
     * 实现过滤黑名单机制
     */
    private static JavaPairDStream<String, String> filterByBlackList(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        // 根据数据库的黑名单,进行实时的过滤,返回格式为:<userId,tup> tup=<offset,(timestamp province city)>
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream.transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
            @Override
            public JavaPairRDD<String, String> call(JavaPairRDD<String, String> v1) throws Exception {
                // 首先从数据库中获取黑名单,并转化为RDD
                IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
                List<AdBlacklist> adBlackLists = adBlacklistDAO.findAll();
                // 封装黑名单用户,格式为:<userId,true>
                List<Tuple2<Long, Boolean>> tuples = new ArrayList<>();
                for (AdBlacklist adBlacklist : adBlackLists) {
                    tuples.add(new Tuple2<>(adBlacklist.getUserid(), true));
                }
                JavaSparkContext sc = new JavaSparkContext(v1.context());
                // 把黑名单信息生成RDD
                JavaPairRDD<Long, Boolean> blackListRDD = sc.parallelizePairs(tuples);
                // 将原始数据RDD映射为<userId,Tuple2<kafka-key,kafka-value>>
                JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = v1.mapToPair(new PairFunction<Tuple2<String, String>, Long, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<Long, Tuple2<String, String>> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                        // 获取用户点击行为数据
                        String log = stringStringTuple2._2;
                        // 原始数据格式为:<offset,(timestamp province city userId adId)>
                        String[] logSplited = log.split(" ");
                        long userId = Long.valueOf(logSplited[3]);
                        return new Tuple2<>(userId, stringStringTuple2);
                    }
                });
                // 将原始日志数据与黑名单RDD进行join,此处需要leftOuterJoin
                // 如果原始日志userId没有在对应的黑名单,一定join不到
                JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedRDD = mappedRDD.leftOuterJoin(blackListRDD);
                // 过滤黑名单
                JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinedRDD.filter(new Function<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> v1) throws Exception {
                        Optional<Boolean> optional = v1._2._2;
                        // 判断是否存在值,如果存在,说明原始日志中的userId join到了某个黑名单用户
                        if (optional.isPresent() && optional.get())
                            return false;
                        return true;
                    }
                });
                // 返回根据黑名单过滤后的数据
                JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> longTuple2Tuple2) throws Exception {
                        return longTuple2Tuple2._2._1;
                    }
                });
                return resultRDD;
            }
        });
        return filteredAdRealTimeLogDStream;
    }
}
