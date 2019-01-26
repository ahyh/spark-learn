package com.yh.spark.learn.ip

import java.sql
import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * IP归属地
  *
  * @author yanhuan
  */
object IPLocations2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ip-locations2").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //在Driver端获取到全部的ip规则数据（全部的ip规则数据就在某一台机器上）
    //读取hdfs上的ip规则
    val ipRules: RDD[String] = sc.textFile(args(0))

    //整理ip规则数据
    val ipRulesRDD: RDD[(Long, Long, String)] = ipRules.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    })

    //将所有的ip规则收集到Driver端
    val rulesInDriver: Array[(Long, Long, String)] = ipRulesRDD.collect()

    //广播规则数据
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rulesInDriver)

    //创建RDD读取访问日志
    val accessLog: RDD[String] = sc.textFile(args(1))

    val provinceAndOne: RDD[(String, Int)] = accessLog.map(line => {
      val fields = line.split("[|]")
      val ip = fields(1)
      //将ip转换成十进制
      val ipNum = IPUtils.ip2Long(ip)
      //进行二分法查找，通过Driver端的引用获取到广播变量
      //通过广播变量的引用就可以拿到当前Executor中的ipRules
      //Task是Driver端生成的，广播变量是伴随着Task发送到Executor中的
      val rulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      //查找对应的
      val index = IPUtils.binarySearch(rulesInExecutor, ipNum)
      var province = "未知"
      if (index != -1) {
        province = rulesInExecutor(index)._3
      }
      (province, 1)
    })

    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey(_ + _)

    //    val result = reduced.collect()
    //
    //    println(result.toBuffer)

    //    reduced.foreach(tp => {
    //      //将数据写入mysql中
    //      //在Executor中执行的Task获取的连接
    //      val conn: Connection = DriverManager.getConnection("jdbc://localhost:3306/yanhuan?charatorEncoding=UTF-8", "root", "123456")
    //      var ps = conn.prepareStatement("...")
    //      ps.setString(1, tp._1)
    //      ps.setInt(2, tp._2)
    //      ps.executeUpdate()
    //      ps.close()
    //      conn.close()
    //    })

    //一个分区拿到一个数据库连接进行操作
    reduced.foreachPartition(it => IPUtils.data2MySQL(it))

    sc.stop()
  }

}
