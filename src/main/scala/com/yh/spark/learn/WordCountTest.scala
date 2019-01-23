package com.yh.spark.learn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark实现word-count
  */
object WordCountTest {

  def main(args: Array[String]): Unit = {
    //创建spark执行入口
    //    val conf = new SparkConf()
    //指定setMaster为local本地执行，可以debug
    val conf = new SparkConf().setAppName("scala-wordcount").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //指定以后从哪里读取数据集，创建RDD
    //    sc.textFile(args(0)).flatMap(_.split(" "))
    //      .map((_, 1)).reduceByKey(_ + _)
    //      .sortBy(_._2, false).saveAsTextFile(args(1))

    //加上类型
    val lines: RDD[String] = sc.textFile(args(0))
    //切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //将单词合一组合
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
    //按key进行聚合
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //将结果保存在HDFS中
    sorted.saveAsTextFile(args(1))
    //释放资源
    sc.stop()
  }

}
