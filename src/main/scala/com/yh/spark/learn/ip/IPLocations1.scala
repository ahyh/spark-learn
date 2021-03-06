package com.yh.spark.learn.ip

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * IP归属地
  *
  * @author yanhuan
  */
object IPLocations1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ip-locations1").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //在Driver端获取到全部的ip规则数据（全部的ip规则数据就在某一台机器上）
    //全部的ip规则在Driver的内存上
    val ipRuels: Array[(Long, Long, String)] = IPUtils.readRules(args(0))

    //将Driver端的数据广播到Executor中
    val broadIpRules: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipRuels)

    //创建RDD读取访问日志
    val accessLog: RDD[String] = sc.textFile(args(1))

    val provinceAndOne: RDD[(String, Int)] = accessLog.map(line => {
      val fields = line.split("[|]")
      val ip = fields(1)
      //将ip转换成十进制
      val ipNum = IPUtils.ip2Long(ip)
      //进行二分法查找，通过Driver端的引用获取到广播变量
      //通过广播变量的引用就可以拿到当前Executor中的ipRules
      val rulesInExecutor: Array[(Long, Long, String)] = broadIpRules.value
      //查找对应的
      val index = IPUtils.binarySearch(rulesInExecutor, ipNum)
      var province = "未知"
      if (index != -1) {
        province = rulesInExecutor(index)._3
      }
      (province, 1)
    })

    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey(_ + _)

    val result = reduced.collect()

    println(result.toBuffer)

    sc.stop()
  }

}
