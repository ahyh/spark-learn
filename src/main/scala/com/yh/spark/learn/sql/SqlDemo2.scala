package com.yh.spark.learn.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SparkSQL的demo2
  *
  * @author yanhuan
  */
object SqlDemo2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sql-1").setMaster("local[4]")

    //创建SparkSQL的连接
    val sc = new SparkContext(conf)
    //将sparkContext包装一下
    val sqlContext = new SQLContext(sc)

    //创建DataFrame
    val lines = sc.textFile("hdfs://mini1:9000/person/person.txt")

    //切分数据
    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val value = fields(3).toDouble
      Row(id, name, age, value)
    })

    //结构类型，就是表头，用于描述dataFrame的
    val schema: StructType = StructType(List(
      StructField("id", LongType, false),
      StructField("name", StringType, false),
      StructField("age", IntegerType, false),
      StructField("value", DoubleType, false)
    ))

    val bdf: DataFrame = sqlContext.createDataFrame(rowRDD,schema)

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
