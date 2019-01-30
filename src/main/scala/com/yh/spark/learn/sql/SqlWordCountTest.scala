package com.yh.spark.learn.sql

import org.apache.spark.sql._

/**
  * SparkSQL的demo2
  * 使用DataFrame的方式使用SparkSQL
  *
  * @author yanhuan
  */
object SqlWordCountTest {

  def main(args: Array[String]): Unit = {

    //创建SparkSession,SparkSQL 2.x编程入口
    val session: SparkSession = SparkSession.builder().appName("spark-sql-2.x").master("local[4]").getOrCreate()

    //Dataset分布式数据集，是对RDD的进一步封装，是更加智能的RDD
    //Dataset只有一列，默认该列名称为value
    val lines: Dataset[String] = session.read.textFile("hdfs://mini1:9000/user/word.txt")

    //切分压平
    //导入隐式转换
    import session.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))

    //    words.createTempView("v_wc")
    //
    //    val result: DataFrame = session.sql("select value,count(*) from v_wc group by value order by count(*) desc")
//    result.show()
    val count: DataFrame = words.groupBy($"value" as "word").count().sort($"count" desc)

    count.show()

    session.stop()

  }

}
