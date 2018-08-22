package com.bay.sessionanalyze.spark.ad;

import com.bay.sessionanalyze.conf.ConfigurationManager;
import com.bay.sessionanalyze.constant.Constants;
import com.bay.sessionanalyze.dao.*;
import com.bay.sessionanalyze.dao.factory.DAOFactory;
import com.bay.sessionanalyze.domain.*;
import com.bay.sessionanalyze.util.DateUtils;
import com.bay.sessionanalyze.util.SparkUtils;
import com.google.common.base.Optional;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
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
        String[] kafkaTopicSplit = kafkaTopics.split(",");
        // 存储topic
        Set<String> topics = new HashSet<>();
        for (String kafkaTopic : kafkaTopicSplit) {
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
        /* 业务一:计算每天各省各城市个广告的点击流量实时统计,返回的格式为:<yyyyMMdd_province_city_adId,clickCount> */
        JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(filterAdRealTimeLogDStream);
        /* 业务二:统计每天各省Top3热门广告 */
        calculateProvinceTop3Ad(adRealTimeStatDStream);
        /* 业务三:统计各广告最近1小时的点击量趋势 */
        calculateClickCountByWindow(filterAdRealTimeLogDStream);

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }

    /**
     * 统计各广告最近1小时的点击量趋势
     */
    private static void calculateClickCountByWindow(JavaPairDStream<String, String> filterAdRealTimeLogDStream) {
        // 首先把数据整合成:<yyyyMMddHHmm_adId,1L>
        JavaPairDStream<String, Long> pairDStream = filterAdRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                // 获取原始数据
                String[] logSplit = stringStringTuple2._2.split(" ");
                // 把时间戳调整为yyyyMMddHHmm
                String timeMinute = DateUtils.formatTimeMinute(new Date(Long.valueOf(logSplit[0])));
                long adId = Long.valueOf(logSplit[4]);
                return new Tuple2<>(timeMinute + "_" + adId, 1L);
            }
        });
        // 每一次出来的新的batch,都要获取最近一小时所有的batch
        // 然后根据key进行聚合,统计出一小时内的个分钟各广告的点击量
        JavaPairDStream<String, Long> aggregatedDStream = pairDStream.reduceByKeyAndWindow(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.minutes(60), Durations.seconds(10));
        // 结果数据的存储
        aggregatedDStream.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Long> v1) throws Exception {
                v1.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> tuple2Iterator) throws Exception {
                        List<AdClickTrend> adClickTrends = new ArrayList<>();
                        while (tuple2Iterator.hasNext()) {
                            Tuple2<String, Long> tuple = tuple2Iterator.next();
                            String[] keySplit = tuple._1.split("_");
                            String dateMinute = keySplit[0];
                            String date = DateUtils.formatDate(DateUtils.parseDateKey(dateMinute.substring(0, 8)));
                            String hour = dateMinute.substring(8, 10);
                            String minute = dateMinute.substring(10);
                            long adId = Long.valueOf(keySplit[1]);
                            long clickCount = tuple._2;
                            AdClickTrend adClickTrend = new AdClickTrend();
                            adClickTrend.setDate(date);
                            adClickTrend.setHour(hour);
                            adClickTrend.setMinute(minute);
                            adClickTrend.setAdid(adId);
                            adClickTrend.setClickCount(clickCount);
                            adClickTrends.add(adClickTrend);
                        }
                        IAdClickTrendDAO adClickTrendDAO = DAOFactory.getAdClickTrendDAO();
                        adClickTrendDAO.updateBatch(adClickTrends);
                    }
                });
                return null;
            }
        });
    }

    /**
     * 统计每天各省Top3热门广告
     */
    private static void calculateProvinceTop3Ad(JavaPairDStream<String, Long> adRealTimeStatDStream) {
        // 把adRealTimeStatDStream数据封装到Row
        // 封装后的数据是没有city字段的
        JavaDStream<Row> rowDStream = adRealTimeStatDStream.transform(new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {
            @Override
            public JavaRDD<Row> call(JavaPairRDD<String, Long> v1) throws Exception {
                // 把v1中的数据格式整合为:<yyyyMMdd_province_adId,clickCount>
                JavaPairRDD<String, Long> mappedRDD = v1.mapToPair(new PairFunction<Tuple2<String, Long>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                        String[] keySplit = stringLongTuple2._1.split("_");
                        String date = keySplit[0];
                        String province = keySplit[1];
                        long adId = Long.valueOf(keySplit[3]);
                        long clickCount = stringLongTuple2._2;
                        String key = date + "_" + province + "_" + adId;
                        return new Tuple2<>(key, clickCount);
                    }
                });
                // 将mappedRDD的clickCount以省份进行聚合,得到省份对应的点击广告数
                JavaPairRDD<String, Long> dailyAdClickCountByProvinceRDD = mappedRDD.reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });
                // 将dailyAdClickCountByProvinceRDD转化成DataFrame
                // 注册成一张临时表
                // 通过SQL的方式(开窗函数)获取,某天各省的top3热门广告
                JavaRDD<Row> rowsRDD = dailyAdClickCountByProvinceRDD.map(new Function<Tuple2<String, Long>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Long> v1) throws Exception {
                        String[] keySplit = v1._1.split("_");
                        String date = keySplit[0];
                        String province = keySplit[1];
                        long adId = Long.valueOf(keySplit[2]);
                        long clickCount = v1._2;
                        return RowFactory.create(date, province, adId, clickCount);
                    }
                });
                // 指定Schema信息
                StructType schema = DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("date", DataTypes.StringType, true),
                        DataTypes.createStructField("province", DataTypes.StringType, true),
                        DataTypes.createStructField("ad_id", DataTypes.LongType, true),
                        DataTypes.createStructField("click_count", DataTypes.LongType, true)
                ));
                // 调用HiveContext,因为SQLContext不支持开窗函数,spark2.0版本之后支持
                HiveContext hiveContext = new HiveContext(v1.context());
                // 映射
                DataFrame dailyAdClickCountByProvinceDF = hiveContext.createDataFrame(rowsRDD, schema);
                dailyAdClickCountByProvinceDF.registerTempTable("tmp_daily_ad_id_click_count_province");
                // 使用HiveContext配合开窗函数,给province打一个行标,统计出各省的Top3热门商品
                String sql = "select " +
                        "date,province,ad_id,click_count " +
                        "from (select " +
                        "date,province,ad_id,click_count," +
                        "row_number() over (partition by province order by click_count desc) as rank " +
                        "from tmp_daily_ad_id_click_count_province) as tmp " +
                        "where rank<=3";
                DataFrame provinceTop3DF = hiveContext.sql(sql);
                return provinceTop3DF.javaRDD();
            }
        });
        // 数据的持久化
        rowDStream.foreachRDD(new Function<JavaRDD<Row>, Void>() {
            @Override
            public Void call(JavaRDD<Row> v1) throws Exception {
                // 对应一个分区的数据
                v1.foreachPartition(new VoidFunction<Iterator<Row>>() {
                    @Override
                    public void call(Iterator<Row> rowIterator) throws Exception {
                        List<AdProvinceTop3> adProvinceTop3s = new ArrayList<>();
                        if (rowIterator.hasNext()) {
                            Row row = rowIterator.next();
                            String date = row.getString(0);
                            String province = row.getString(1);
                            long adId = row.getLong(2);
                            long clickCount = row.getLong(3);
                            AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3();
                            adProvinceTop3.setDate(date);
                            adProvinceTop3.setProvince(province);
                            adProvinceTop3.setAdid(adId);
                            adProvinceTop3.setClickCount(clickCount);
                            adProvinceTop3s.add(adProvinceTop3);
                        }
                        IAdProvinceTop3DAO adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO();
                        adProvinceTop3DAO.truncate();
                        adProvinceTop3DAO.updateBatch(adProvinceTop3s);
                    }
                });
                return null;
            }
        });
    }

    /**
     * 计算每天各省各城市个广告的点击流量实时统计
     */
    private static JavaPairDStream<String, Long> calculateRealTimeStat(JavaPairDStream<String, String> filterAdRealTimeLogDStream) {
        // 注意:计算该业务,会实时的更新数据库,数据平台会实时的把结果展示出来
        // 对原始数据进行map操作,生成格式为:<date_province_city_adId,1L>
        JavaPairDStream<String, Long> mappedDStream = filterAdRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                // stringStringTuple2中的数据格式为:<key,<timestamp province city userId adId>>
                String[] logSplit = stringStringTuple2._2.split(" ");
                // 获取date,province,city,adId
                String timeStamp = logSplit[0];
                Date date = new Date(Long.valueOf(timeStamp));
                String dateKey = DateUtils.formatDateKey(date);
                String province = logSplit[1];
                String city = logSplit[2];
                String adId = logSplit[4];
                String key = dateKey + "_" + province + "_" + city + "_" + adId;
                return new Tuple2<>(key, 1L);
            }
        });
        // 进行聚合,需要按批次累加历史结果
        // 在这个DStream中,相当于每天各省个城市各广告的点击次数
        JavaPairDStream<String, Long> aggregatedDStream = mappedDStream.updateStateByKey(new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
            @Override
            public Optional<Long> call(List<Long> v1, Optional<Long> v2) throws Exception {
                // optional是历史批次结果数据
                // 首先根据optional判断,之前的这个key是否有值
                long clickCount = 0L;
                // 如果之前存在值,就以之前的状态作为起点,进行值的累加
                if (v2.isPresent()) {
                    clickCount = v2.get();
                }
                // values代表当前batch中每个key对应的所有的值
                // 你如点击量是4,values=(1,1,1,1)
                for (Long value : v1) {
                    clickCount += value;
                }
                return Optional.of(clickCount);
            }
        });
        // 结果存入数据库
        aggregatedDStream.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Long> v1) throws Exception {
                v1.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> tuple2Iterator) throws Exception {
                        List<AdStat> adStats = new ArrayList<>();
                        while (tuple2Iterator.hasNext()) {
                            Tuple2<String, Long> tuple = tuple2Iterator.next();
                            String[] keySplit = tuple._1.split("_");
                            String date = keySplit[0];
                            String province = keySplit[1];
                            String city = keySplit[2];
                            long adId = Long.valueOf(keySplit[3]);
                            long clickCount = tuple._2;
                            AdStat adStat = new AdStat();
                            adStat.setDate(date);
                            adStat.setProvince(province);
                            adStat.setCity(city);
                            adStat.setAdid(adId);
                            adStat.setClickCount(clickCount);
                            adStats.add(adStat);
                        }
                        IAdStatDAO adStatDAO = DAOFactory.getAdStatDAO();
                        adStatDAO.updateBatch(adStats);
                    }
                });
                return null;
            }
        });
        return aggregatedDStream;
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
                String[] logSplit = log.split(" ");
                // 获取yyyyMMdd userId adId
                String timeStamp = logSplit[0];
                Date date = new Date(Long.valueOf(timeStamp));
                String dateKey = DateUtils.formatDateKey(date); // yyyyMMdd
                long userId = Long.valueOf(logSplit[3]);
                long adId = Long.valueOf(logSplit[4]);
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
        JavaPairDStream<String, Long> blackListDStream = dailyUserAdClickCountDStream.filter(new Function<Tuple2<String, Long>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Long> v1) throws Exception {
                String[] keySplit = v1._1.split("_");
                // 获取date,userId,adId
                String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplit[0]));
                long userId = Long.valueOf(keySplit[1]);
                long adId = Long.valueOf(keySplit[2]);
                IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                // 从数据库中获取某个日期中某个用户对某个广告的点击量
                int clickCount = adUserClickCountDAO.findClickCountByMultiKey(date, userId, adId);
                // 如果点击量大于等于100,就把该用户拉入黑名单
                if (clickCount >= 100)
                    return true;
                return false;
            }
        });
        /*
         * 将黑名单用户更新到数据库
         * 注意:blackListDStream中可能有重复的用户,需要去重
         * */
        // 首先获取userId
        JavaDStream<Long> blackListUserIdDStream = blackListDStream.map(new Function<Tuple2<String, Long>, Long>() {
            @Override
            public Long call(Tuple2<String, Long> v1) throws Exception {
                String[] keySplit = v1._1.split("_");
                Long userId = Long.valueOf(keySplit[1]);
                return userId;
            }
        });
        // 根据userId进行去重
        JavaDStream<Long> distinctBlackUserIdDStream = blackListUserIdDStream.transform(new Function<JavaRDD<Long>, JavaRDD<Long>>() {
            @Override
            public JavaRDD<Long> call(JavaRDD<Long> v1) throws Exception {
                return v1.distinct();
            }
        });
        // 将黑名单存储到数据库
        distinctBlackUserIdDStream.foreachRDD(new Function<JavaRDD<Long>, Void>() {
            @Override
            public Void call(JavaRDD<Long> v1) throws Exception {
                v1.foreachPartition(new VoidFunction<Iterator<Long>>() {
                    @Override
                    public void call(Iterator<Long> longIterator) throws Exception {
                        List<AdBlacklist> adBlacklists = new ArrayList<>();
                        while (longIterator.hasNext()) {
                            long userId = longIterator.next();
                            AdBlacklist adBlacklist = new AdBlacklist();
                            adBlacklist.setUserid(userId);
                            adBlacklists.add(adBlacklist);
                        }
                    }
                });
                return null;
            }
        });

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
                        String[] logSplit = log.split(" ");
                        long userId = Long.valueOf(logSplit[3]);
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
