package com.yh.spark.learn.favteacher

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求最受欢迎的老师
  *
  * GroupFavTeacher中需要toList，如果数据量非常大，消耗内存
  */
object Group2FavTeacher {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("fav-teacher").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //设置checkpoint目录为hdfs中的一个目录
    sc.setCheckpointDir("hdfs://mini1:9000/spark/ck")

    //指定从哪读取数据
    val lines: RDD[String] = sc.textFile(args(0))

    //整理数据
    val subjectAndTeacher: RDD[((String, String), Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })

    //聚合，将学科，老师联合起来当做一个key
    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(_ + _)

    //将RDDcache到内存中
    val cached = reduced.cache()

    //保存点，保存中间结果
    reduced.checkpoint()

    //scala的集合排序是在内存中进行的，但是内存有可能不够用
    //可以调用RDD的sortBy方法，使用磁盘+内存的方式排序
    val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1 == "bigdata")

    //调用RDD的sortBy方法
    val top: Array[((String, String), Int)] = filtered.sortBy(_._2, false).take(3)

    println(top.toBuffer)

    sc.stop()
  }

}
