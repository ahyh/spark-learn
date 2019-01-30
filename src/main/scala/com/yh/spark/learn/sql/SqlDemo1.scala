package com.yh.spark.learn.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SqlDemo1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sql-1").setMaster("local[4]")

    //创建SparkSQL的连接
    val sc = new SparkContext(conf)
    //将sparkContext包装一下
    val sqlContext = new SQLContext(sc)

    //创建DataFrame
    val lines = sc.textFile("hdfs://mini1:9000/person/person.txt")

    //切分数据
    val boyRDD: RDD[Boy] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toInt
      val name = fields(1)
      val age = fields(2).toInt
      val value = fields(3).toDouble
      Boy(id, name, age, value)
    })

    //导入隐式转换
    import sqlContext.implicits._
    val bdf: DataFrame = boyRDD.toDF

    //变成DataFrame后就可以使用API编程了
    //将DataFrame注册成表
    bdf.registerTempTable("t_boy")

    //通过SQL编程
    val result: DataFrame = sqlContext.sql("select * from t_boy order by value desc,age desc")

    //查看结果，触发Action
    result.show()

    sc.stop()

  }

}

case class Boy(id: Int, name: String, age: Int, value: Double)
