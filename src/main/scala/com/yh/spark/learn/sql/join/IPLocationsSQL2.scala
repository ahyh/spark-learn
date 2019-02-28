package com.yh.spark.learn.sql.join

import com.yh.spark.learn.ip.IPUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * IP归属地
  * join的代价非常昂贵，且非常慢，所以将表缓存起来作为广播变量
  *
  * @author yanhuan
  */
object IPLocationsSQL2 {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("IPLocationsSQL").master("local[4]").getOrCreate()

    //在Driver端获取到全部的ip规则数据（全部的ip规则数据就在某一台机器上）
    //读取hdfs上的ip规则
    import session.implicits._
    val ipRules: Dataset[String] = session.read.textFile(args(0))

    //整理ip规则数据
    val ruleDataset: Dataset[(Long, Long, String)] = ipRules.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    })

    //收集ip规则到Driver端
    val rulesInDriver: Array[(Long, Long, String)] = ruleDataset.collect()

    //广播出去
    //将广播变量的引用返回到Driver端
    val broadCastRef: Broadcast[Array[(Long, Long, String)]] = session.sparkContext.broadcast(rulesInDriver)

    //创建RDD读取访问日志
    val accessLog: Dataset[String] = session.read.textFile(args(1))

    val ipDataFrame: DataFrame = accessLog.map(log => {
      val fields = log.split("[|]")
      val ip = fields(1)
      val ipNum = IPUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_num")

    ipDataFrame.createTempView("v_log")

    //自定义函数并注册,该函数的功能是输入ip地址对应的十进制，返回省份的名称
    session.udf.register("ip2Province", (ipNum: Long) => {
      //查找ip规则（事先已经广播了，已经存在业务逻辑中了）
      //函数的逻辑代码在Executor中执行，使用广播变量的引用就可以使用了
      val ipRulesInExecutor: Array[(Long, Long, String)] = broadCastRef.value
      //根据IP地址对应的十进制查找省份名称
      val index = IPUtils.binarySearch(ipRulesInExecutor, ipNum)
      var province = "未知"
      if (index != -1) {
        province = ipRulesInExecutor(index)._3
      }
      province
    })

    //执行SQL
    val result = session.sql("select ip2Province(ip_num) province, COUNT(*) counts FROM v_log GROUP BY province ORDER BY counts DESC")

    // val result = session.sql("SELECT ip2Province(ip_num) province, COUNT(*) counts FROM v_log GROUP BY province ORDER BY counts DESC")

    result.show()

    session.stop()
  }

}
