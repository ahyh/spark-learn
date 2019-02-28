package com.yh.spark.learn.sql.join

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object SparkSQLJoinTest {

  def main(args: Array[String]): Unit = {

    //获取SparkSession对象
    val session: SparkSession = SparkSession.builder().appName("JoinTest").master("local[*]").getOrCreate()

    //准备初始数据
    import session.implicits._
    val lines: Dataset[String] = session.createDataset(List("1,lao zhao,China", "2,lao duan,America"))

    //处理数据
    val tpDs: Dataset[(Long, String, String)] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val country = fields(2)
      (id, name, country)
    })

    //国家数据
    val countrys: Dataset[String] = session.createDataset(List("China,中国", "America,美国"))

    //处理数据
    val countrysDs: Dataset[(String, String)] = countrys.map(line => {
      val fields = line.split(",")
      val country = fields(0)
      val name = fields(1)
      (country, name)
    })

    //转换成DataFrame
    val df1 = tpDs.toDF("id", "name", "country")
    val countryDf = countrysDs.toDF("con_country", "con_name")

    //两表关联：第一种方式是注册成视图的方式
    df1.createTempView("v_user")
    countryDf.createTempView("v_country")
    val result: DataFrame = session.sql("select id,name,con_name from v_user join v_country on country = con_country")


    //两表关联：第二种方式是dataframe
    val frame = df1.join(countryDf, $"country" === $"con_country", "left")

    df1.show()

    result.show()

    frame.show()

    session.stop()

  }

}
