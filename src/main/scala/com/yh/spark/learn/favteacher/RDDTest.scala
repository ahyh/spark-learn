package com.yh.spark.learn.favteacher

import org.apache.spark.{SparkConf, SparkContext}

object RDDTest {

  def main(args: Array[String]): Unit = {
    //创建spark执行入口
    // val conf = new SparkConf()
    //指定setMaster为local本地执行，可以debug
    val conf = new SparkConf().setAppName("rdd-test").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 2)
    println(rdd.partitions.length)

    val rdd2 = rdd.mapPartitionsWithIndex((index, it) => {
      it.map(x => s"part:$index,ele:$x")
    })

    val arr = rdd2.collect()
  }

}
