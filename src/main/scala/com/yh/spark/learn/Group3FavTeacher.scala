package com.yh.spark.learn

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 将相同学科的数据放在一个分区中
  *
  * 自定义分区器
  *
  * @author yanhuan
  */
object Group3FavTeacher {

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

    //计算有多少学科
    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()

    //自定义一个分区器
    val subjectPartitioner = new SubjectPartitioner(subjects)

    //自定义分区器，并且按照自定义的分区器进行分区
    //partitionBy按照指定的分区器进行分区
    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(subjectPartitioner)

    //一次拿出一个分区进行计算
    val sorted: RDD[((String, String), Int)] = partitioned.mapPartitions(it => {
      //将迭代器转换成List进行排序，在转换成迭代器返回
      it.toList.sortBy(_._2).take(3).iterator
    })

    val result: Array[((String, String), Int)] = sorted.collect()

    println(result.toBuffer)

    sc.stop()
  }

}

/**
  * 自定义的分区器
  */
class SubjectPartitioner(subjects: Array[String]) extends Partitioner {

  val rules = new mutable.HashMap[String, Int]()
  var i: Int = 0
  for (sb <- subjects) {
    rules(sb) = i
    i += 1
  }

  /**
    * 返回分区的数量（下一个RDD有多少分区）
    */
  override def numPartitions: Int = subjects.length

  /**
    * 根据传入的key计算分区标号
    * key是一个(String,String)的元组
    */
  override def getPartition(key: Any): Int = {
    //获取学科名称
    val subject = key.asInstanceOf[(String, String)]._1
    //根据规则计算学科编号
    rules(subject)
  }
}
