package com.yh.spark.learn.sql.join

import com.yh.spark.learn.ip.IPUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * IP归属地
  *
  * @author yanhuan
  */
object IPLocationsSQL {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("IPLocationsSQL").master("local[4]").getOrCreate()

    //在Driver端获取到全部的ip规则数据（全部的ip规则数据就在某一台机器上）
    //读取hdfs上的ip规则
    import session.implicits._
    val ipRules: Dataset[String] = session.read.textFile(args(0))

    //整理ip规则数据
    val ruleDataFrame: DataFrame = ipRules.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    }).toDF("snum", "enum", "province")

    //创建RDD读取访问日志
    val accessLog: Dataset[String] = session.read.textFile(args(1))

    val ipDataFrame: DataFrame = accessLog.map(log => {
      val fields = log.split("[|]")
      val ip = fields(1)
      val ipNum = IPUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_num")

    ruleDataFrame.createTempView("v_rules")
    ipDataFrame.createTempView("v_ips")
    val result = session.sql("select province,count(*) counts from v_ips join v_rules on (ip_num >= snum and ip_num <= enum) group by province")

    result.show()

    session.stop()
  }

}
