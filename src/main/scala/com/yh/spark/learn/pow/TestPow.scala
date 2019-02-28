package com.yh.spark.learn.pow

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object TestPow {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("test-pow").master("local[*]").getOrCreate()
    val range = spark.range(1, 11)

    //将range注册成视图
    range.createTempView("v_range")

    val geoMean = new GeoMean

//    //注册函数
//    spark.udf.register("gm", geoMean)
//
//    val result = spark.sql("select gm(id) from v_range")
    import spark.implicits._
    val result = range.agg(geoMean($"id").as("geoMean")).show()

    spark.stop()
  }

}

/**
  * 自定义聚合函数
  */
class GeoMean extends UserDefinedAggregateFunction {

  //输入数据类型
  override def inputSchema: StructType = StructType(List(StructField("value", DoubleType)))

  //产生中间结果的数据类型
  override def bufferSchema: StructType = StructType(List(
    StructField("counts", LongType), //参与运算的个数
    StructField("product", DoubleType))) //中间运算的结果

  //最终返回结果数据类型
  override def dataType: DataType = DoubleType

  //确保一致性
  override def deterministic: Boolean = true

  //指定初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L //参与运算个数
    buffer(1) = 1.0 //运算出的中间值
  }

  //每有一条数据参与运算就更新一下中间结果
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + 1L //参与运算的个数
    buffer(1) = buffer.getDouble(1) * input.getDouble(0) //中间结果的乘积
  }

  //中间结果相加和相乘
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getDouble(1) * buffer2.getDouble(1)
  }

  //计算最终结果
  override def evaluate(buffer: Row): Any = {
    math.pow(buffer.getDouble(1), 1.toDouble / buffer.getLong(0))
  }
}
