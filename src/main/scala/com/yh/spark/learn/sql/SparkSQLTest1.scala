package com.yh.spark.learn.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Spark 2.x的SparkSQL编程
  *
  * @author yanhuan
  */
object SparkSQLTest1 {

  def main(args: Array[String]): Unit = {

    //创建SparkSession,SparkSQL 2.x编程入口
    val session: SparkSession = SparkSession.builder().appName("spark-sql-2.x").master("local[4]").getOrCreate()

    //创建RDD
    val lines: RDD[String] = session.sparkContext.textFile("hdfs://mini1:9000/person/person.txt")

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

    //创建DataFrame
    val bdf: DataFrame = session.createDataFrame(rowRDD, schema)

    //导入隐式转换
    import session.implicits._
    val newDf: Dataset[Row] = bdf.where($"value" > 90).orderBy($"value" desc, $"age" desc)

    newDf.show()

    //关闭session
    session.stop()
  }

}
