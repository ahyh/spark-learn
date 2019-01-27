package com.yh.spark.learn.sort

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CustomerSort2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sort1").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val users = Array("laoduan 30 99", "laozhao 28 88", "laohu 31 99", "laowu 27 97", "xiaofang 26 100")

    val lines: RDD[String] = sc.parallelize(users)

    //切分整理数据
    val userRdd: RDD[(String, Int, Int)] = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val value = fields(2).toInt
      //      new User(name, age, value)
      (name, age, value)
    })

    //获取User对象的RDD后的排序对象
    //    val sorted: RDD[User] = userRdd.sortBy(u => u)
    //利用元组的特性进行排序,先比第一个，如果相等则比第二个
    val sorted = userRdd.sortBy(tp => (tp._3,tp._2))

//    //根据case类进行排序
//    val sortedWithCaseClass = userRdd.sortBy(u => UserCaseCompare(u._2, u._3))

    val result = sorted.collect()

    println(result.toBuffer)

    sc.stop()
  }

}
