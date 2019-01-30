package com.yh.spark.learn.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SparkSQL的demo2
  *
  * @author yanhuan
  */
object SqlDemo3 {

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

    val bdf: DataFrame = sqlContext.createDataFrame(rowRDD, schema)

    //直接使用DataFrame API的方式
    val newBdf: DataFrame = bdf.select("name", "age", "value")

    //导入隐式转换
    import sqlContext.implicits._

    val orderedBdf: Dataset[Row] = newBdf.orderBy($"value" desc, $"age" desc)

    //查看结果，触发Action
    newBdf.show()

    //查看排序后的结果
    orderedBdf.show()

    sc.stop()

  }

}
