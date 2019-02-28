package com.yh.spark.learn.jdbc

import org.apache.spark.sql.{DataFrame, SparkSession}

object JdbcDatasourceSQL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JdbcDataSource")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //load这个方法会读取真正mysql的数据吗？
    //load这个方法
    val salarys: DataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://localhost:3306/yanhuan",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "salary",
        "user" -> "root",
        "password" -> "123456")
    ).load()

    //获取表的schema信息
    salarys.printSchema()

    salarys.show()

    spark.close()
  }

}
