package com.yh.spark.learn.favteacher

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求每个学科最受欢迎的老师
  *
  * @author huanyan
  */
object GroupFavTeacher {

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

    //聚合，将学科，老师联合起来当做一个key
    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(_ + _)

    //按学科进行分组，相同集合的数据卸载迭代器中
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy((t: ((String, String), Int)) => t._1._1, 4)

    //经过分组后，一个分区内可能有多个学科的数据，一个学科就是一个迭代器
    //将每一个组拿出来进去排序
    //因为一个学科的数据已经在一台机器上的scala集合中了，所以可以调用scala的sortBy
    val sorted: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(3))

    //收集结果
    val result: Array[(String, List[((String, String), Int)])] = sorted.collect()

    println(result.toBuffer)

    sc.stop()
  }

}
