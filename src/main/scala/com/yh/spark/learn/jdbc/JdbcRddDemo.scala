package com.yh.spark.learn.jdbc

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JdbcRddDemo {

  /**
    * 定义一个函数变量
    */
  val getConn = () => {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/yanhuan?characterEncoding=UTF-8", "root", "123456")
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("jdbc-demo").setMaster("local[4]")

    val sc = new SparkContext(conf)

    //new出来的rdd里面没有数据，而是告诉rdd以后触发Action从哪里读取数据
    val jdbcRdd: JdbcRDD[(Int, String, Int, String, java.math.BigDecimal)] = new JdbcRDD(sc, getConn, "select * from salary where id >= ? and id <= ?", 12, 100, 4, rs => {
      val id = rs.getInt(1)
      val name = rs.getString(2)
      val age = rs.getInt(3)
      val sex = rs.getInt(4)
      val company = rs.getString(5)
      val salary = rs.getBigDecimal(6)
      (id, name, sex, company, salary)
    })

    //触发Action
    val r: Array[(Int, String, Int, String, java.math.BigDecimal)] = jdbcRdd.collect()

    println(r.toBuffer)

    sc.stop()

  }


}
