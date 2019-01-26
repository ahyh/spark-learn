package com.yh.spark.learn

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 将相同学科的数据放在一个分区中
  *
  * 自定义分区器,减少shuffle
  *
  * @author yanhuan
  */
object Group4FavTeacher {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("fav-teacher").setMaster("local[4]")
    val sc = new SparkContext(conf)

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

    val subjects: Array[String] = subjectAndTeacher.map(_._1._1).distinct().collect()

    //自定义一个分区器
    val subjectPartitioner = new SubjectPartitioner(subjects)

    //聚合时按照指定分区器聚合
    //聚合，将学科，老师联合起来当做一个key
    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(subjectPartitioner, _ + _)

    //一次拿出一个分区进行计算
    val sorted: RDD[((String, String), Int)] = reduced.mapPartitions(it => {
      //将迭代器转换成List进行排序，在转换成迭代器返回
      //排序，但是不全部加载到
      it.toList.sortBy(_._2).take(3).iterator
    })

    val result: Array[((String, String), Int)] = sorted.collect()

    println(result.toBuffer)

    sc.stop()
  }

}

