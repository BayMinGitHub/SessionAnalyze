package com.bay.scala.sessionanalyze.spark.session

import java.lang
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bay.sessionanalyze.constant.Constants
import com.bay.sessionanalyze.dao.factory.DAOFactory
import com.bay.sessionanalyze.domain.{SessionAggrStat, SessionRandomExtract}
import com.bay.sessionanalyze.util._
import org.apache.commons.collections.IteratorUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Accumulable, SparkConf, SparkContext}

import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Author by BayMin, Date on 2018/8/16.
  */
object SessionAnalyzeSpark {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("SessionAnalyzeSpark")
    SparkUtils.setMaster(conf)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    SparkUtils.mockData(sc, sqlContext)
    val taskDAO = DAOFactory.getTaskDAO
    val taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION)
    val task = taskDAO.findById(taskId)
    if (task == null) {
      throw new RuntimeException(new Date + "找不到任务ID")
    }
    val taskParam = JSON.parseObject(task.getTaskParam)
    val actionRDD: RDD[Row] = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam).rdd
    val sessionId2ActionRDD: RDD[(String, Row)] = actionRDD.map(line => (line.getString(2), line))
    sessionId2ActionRDD.cache()
    val userInfoRDD: RDD[Row] = sqlContext.sql("select * from user_info").rdd
    val userId2userInfoRDD: RDD[(Long, Row)] = userInfoRDD.map(line => (line.getLong(0), line))
    val userId2PartAggrInfoRDD: RDD[(Long, String)] = getuserId2PartAggrInfo(sessionId2ActionRDD)
    val sessionId2FullAggrInfoRDD: RDD[(String, String)] = getSessionId2FullAggrInfo(userId2PartAggrInfoRDD, userId2userInfoRDD)
    val sessionAggrStatAccumulator = sc.accumulable("")(SessionAnalyzeAccumulator)
    val parameter = getparmeter(taskParam)
    val filterdSessionAggrInfoRDD: RDD[(String, String)] = getFilterSessionAggrInfo(sessionId2FullAggrInfoRDD, parameter, sessionAggrStatAccumulator)
    filterdSessionAggrInfoRDD.persist(StorageLevel.MEMORY_AND_DISK)
    val sessionId2DetailRDD: RDD[(String, Row)] = filterdSessionAggrInfoRDD.join(sessionId2ActionRDD).map(line => (line._1, line._2._2))
    sessionId2DetailRDD.cache()
    println(sessionId2DetailRDD.count())
    calcalateAndPersistAggrStat(sessionAggrStatAccumulator.value, taskId)
    randomExtranctSession(sc, task.getTaskid, filterdSessionAggrInfoRDD, sessionId2DetailRDD);
  }

  def randomExtranctSession(sc: SparkContext, getTaskid: Long, filterdSessionAggrInfoRDD: RDD[(String, String)], sessionId2DetailRDD: RDD[(String, Row)]): Unit = {
    val dateHourSessionIdRDD: RDD[((String, String), String)] = filterdSessionAggrInfoRDD.map(line => {
      val aggrInfo = line._2
      val startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME)
      val dateHour = DateUtils.getDateHour(startTime)
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)
      ((date, hour), aggrInfo)
    })
    val countSession = dateHourSessionIdRDD.count()
    val countHour = dateHourSessionIdRDD.countByKey()
    val dateHour2SessionIds: RDD[((String, String), Iterable[String])] = dateHourSessionIdRDD.groupByKey()
    val dateHourTime: RDD[((String, String), Int)] = dateHour2SessionIds.map(line => {
      val dateHour = line._1
      val count = line._2.iterator.length
      (dateHour, count)
    })
    dateHour2SessionIds.join(dateHourTime).map(line => {
      val iterator = line._2._1.iterator
      val count = line._2._2
      count.toDouble / countSession.toDouble
    })


    //    val sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO
    //    unit.map(line => {
    //      val extract = new SessionRandomExtract
    //      extract.setTaskid(getTaskid)
    //      extract.setSessionid(StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_SESSION_ID))
    //      extract.setStartTime(StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_START_TIME))
    //      extract.setSearchKeywords(StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_SEARCH_KEYWORDS))
    //      extract.setClickCategoryIds(StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS))
    //      sessionRandomExtractDAO.insert(extract)
    //    })
  }

  def calcalateAndPersistAggrStat(value: String, taskId: lang.Long): Unit = {
    // 首先从Accumulator统计的字符串结果中获取各个值
    val session_count = StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT).toLong
    val visit_length_1s_3s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s).toLong
    val visit_length_4s_6s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s).toLong
    val visit_length_7s_9s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s).toLong
    val visit_length_10s_30s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s).toLong
    val visit_length_30s_60s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s).toLong
    val visit_length_1m_3m = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m).toLong
    val visit_length_3m_10m = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m).toLong
    val visit_length_10m_30m = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m).toLong
    val visit_length_30m = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m).toLong

    val step_length_1_3 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3).toLong
    val step_length_4_6 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6).toLong
    val step_length_7_9 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9).toLong
    val step_length_10_30 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30).toLong
    val step_length_30_60 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60).toLong
    val step_length_60 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60).toLong

    // 计算各个访问时长和访问步长的范围占比
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s.toDouble / session_count.toDouble, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s.toDouble / session_count.toDouble, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s.toDouble / session_count.toDouble, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s.toDouble / session_count.toDouble, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s.toDouble / session_count.toDouble, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m.toDouble / session_count.toDouble, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m.toDouble / session_count.toDouble, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m.toDouble / session_count.toDouble, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m.toDouble / session_count.toDouble, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3.toDouble / session_count.toDouble, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6.toDouble / session_count.toDouble, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9.toDouble / session_count.toDouble, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30.toDouble / session_count.toDouble, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60.toDouble / session_count.toDouble, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60.toDouble / session_count.toDouble, 2)

    // 将统计结果封装到Domain对象里
    val sessionAggrStat = new SessionAggrStat()
    sessionAggrStat.setTaskid(taskId)
    sessionAggrStat.setSession_count(session_count)
    sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio)
    sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio)
    sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio)
    sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio)
    sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio)
    sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio)
    sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio)
    sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio)
    sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio)
    sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio)
    sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio)
    sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio)
    sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio)
    sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio)
    sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio)

    // 结果存储
    val sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO()
    sessionAggrStatDAO.insert(sessionAggrStat)
  }

  def getFilterSessionAggrInfo(sessionId2FullAggrInfoRDD: RDD[(String, String)], parameter: String, sessionAggrStatAccumulator: Accumulable[String, String]): RDD[(String, String)] = {
    sessionId2FullAggrInfoRDD.filter(line => {
      val aggrInfo = line._2
      /*依次按照筛选条件进行过滤*/
      // 年龄
      if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) false
      // 职业
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) false
      // 城市
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) false
      // 性别
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) false
      // 关键字
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) false
      // 点击品类
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) false
      sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)
      val visitLength = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
      val stepLength = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
      // 计算访问时长范围
      calculateVisitLength(visitLength, sessionAggrStatAccumulator)
      // 计算访问步长范围
      calculateStepLength(stepLength, sessionAggrStatAccumulator)
      true
    })
  }

  def calculateVisitLength(visitLength: Long, sessionAggrStatAccumulator: Accumulable[String, String]) = {
    if (visitLength >= 1 && visitLength <= 3) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    else if (visitLength >= 4 && visitLength <= 6) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    else if (visitLength >= 7 && visitLength <= 9) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    else if (visitLength >= 10 && visitLength < 30) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    else if (visitLength >= 30 && visitLength < 60) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    else if (visitLength >= 60 && visitLength < 180) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    else if (visitLength >= 180 && visitLength < 600) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    else if (visitLength >= 600 && visitLength < 1800) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    else if (visitLength >= 1800) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m)
  }

  def calculateStepLength(stepLength: Long, sessionAggrStatAccumulator: Accumulable[String, String]) = {
    if (stepLength >= 1 && stepLength <= 3) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3)
    else if (stepLength >= 4 && stepLength <= 6) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6)
    else if (stepLength >= 7 && stepLength <= 9) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9)
    else if (stepLength >= 10 && stepLength < 30) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30)
    else if (stepLength >= 30 && stepLength < 60) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60)
    else if (stepLength >= 60) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60)
  }

  def getuserId2PartAggrInfo(sessionId2ActionRDD: RDD[(String, Row)]): RDD[(Long, String)] = {
    sessionId2ActionRDD.groupByKey().map(line => {
      val sessionId = line._1
      val iterator = line._2.iterator
      var stepLength = 0
      var serachKeywords = ""
      var clickCategoryIds = ""
      val defalut = new Date()
      var startTime = defalut
      var endTime = defalut
      val useId = 0L
      while (iterator.hasNext) {
        val row = iterator.next()
        val useId = row.getLong(1)
        val searchKeyword = row.getString(5)
        val clickCategroyId = row.getLong(6).toString
        val actionTime = DateUtils.parseTime(row.getString(4))
        if (StringUtils.isNotEmpty(searchKeyword)) if (!serachKeywords.contains(searchKeyword)) serachKeywords += searchKeyword + ","
        if (clickCategroyId != null) if (!clickCategoryIds.contains(clickCategroyId)) clickCategoryIds += clickCategroyId + ","
        if (startTime == defalut) startTime = actionTime
        if (endTime == defalut) endTime = actionTime
        if (actionTime.before(startTime)) startTime = actionTime
        if (actionTime.after(endTime)) endTime = actionTime
        stepLength += 1
      }
      serachKeywords = StringUtils.trimComma(serachKeywords)
      clickCategoryIds = StringUtils.trimComma(clickCategoryIds)
      val visitLength = (endTime.getTime - startTime.getTime) / 1000
      val partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
        Constants.FIELD_SEARCH_KEYWORDS + "=" + serachKeywords + "|" +
        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" +
        Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
        Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
        Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime) + "|"
      (useId, partAggrInfo)
    })
  }

  def getparmeter(taskParam: JSONObject): String = {
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val catgorys = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)
    var _parameter = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (catgorys != null) Constants.PARAM_CATEGORY_IDS + "=" + catgorys + "|" else "")
    if (_parameter.endsWith("\\|")) _parameter = _parameter.substring(0, _parameter.length() - 1)
    _parameter
  }

  def getSessionId2FullAggrInfo(userId2PartAggrInfoRDD: RDD[(Long, String)], userId2userInfoRDD: RDD[(Long, Row)]): RDD[(String, String)] = {
    userId2PartAggrInfoRDD.join(userId2userInfoRDD).map(line => {
      val partAggrInfo = line._2._1
      val userInfoRow = line._2._2
      val sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID)
      val age = userInfoRow.getInt(3)
      val professional = userInfoRow.getString(4)
      val city = userInfoRow.getString(5)
      val sex = userInfoRow.getString(6)
      val fullAggrInfo = partAggrInfo +
        Constants.FIELD_AGE + "=" + age + "|" +
        Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
        Constants.FIELD_CITY + "=" + city + "|" +
        Constants.FIELD_SEX + "=" + sex + "|"
      (sessionId, fullAggrInfo)
    })
  }
}