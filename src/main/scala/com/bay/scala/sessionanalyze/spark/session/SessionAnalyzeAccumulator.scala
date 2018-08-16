package com.bay.scala.sessionanalyze.spark.session

import com.bay.sessionanalyze.constant.Constants
import com.bay.sessionanalyze.util.StringUtils
import org.apache.spark.AccumulatorParam

/**
  * Author by BayMin, Date on 2018/8/16.
  */
object SessionAnalyzeAccumulator extends AccumulatorParam[String] {

  override def addInPlace(r1: String, r2: String): String = {
    add(r1, r2)
  }

  override def zero(initialValue: String): String = {
    Constants.SESSION_COUNT + "=0|" +
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
      Constants.STEP_PERIOD_60 + "=0|"
  }

  def add(r1: String, r2: String): String = {
    if (StringUtils.isEmpty(r1))
      return r2

    // 如果r1不为空，从r1中提取r2对应的值，然后累加1
    val oldValue = StringUtils.getFieldFromConcatString(r1, "\\|", r2)
    if (oldValue != null) {
      // 将范围区间原有的值转换为int类型后累加1
      val newValue = Integer.valueOf(oldValue) + 1
      // 将r1中r2对应的值，更新为新的累加后的值
      return StringUtils.setFieldInConcatString(r1, "\\|", r2, String.valueOf(newValue))
    }
    r1
  }

}
