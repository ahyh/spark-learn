package com.yh.spark.learn.favteacher

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求最受欢迎的老师
  *
  * @author yanhuan
  */
object FavTeacher {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("fav-teacher").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //指定从哪读取数据
    val lines: RDD[String] = sc.textFile(args(0))

    //整理数据
    val teacherAndOne: RDD[(String, Int)] = lines.map(line => {
      val teacher = line.substring(line.lastIndexOf("/") + 1)
      (teacher, 1)
    })

    //聚合
    val reduced: RDD[(String, Int)] = teacherAndOne.reduceByKey(_ + _)

    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)

    //执行计算
    val result: Array[(String, Int)] = sorted.collect()

    //打印
    println(result.toBuffer)

    sc.stop()

  }

}
