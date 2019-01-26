package com.yh.spark.learn.ip

import java.sql.{Connection, DriverManager, PreparedStatement}

import scala.io.{BufferedSource, Source}

/**
  * 计算ip地址的归属地
  *
  * @author huanyan
  */
object IPUtils {

  def main(args: Array[String]): Unit = {
    val ipRules: Array[(Long, Long, String)] = readRules("file/ip.txt")
    println(binarySearch(ipRules, ip2Long("115.120.36.118")))
  }

  /**
    * 二分查找
    */
  def binarySearch(lines: Array[(Long, Long, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2)) {
        return middle
      }
      if (ip < lines(middle)._1) {
        high = middle - 1
      } else {
        low = middle + 1
      }
    }
    -1
  }

  /**
    * 读取ip规则，整理后放入到内存中
    */
  def readRules(path: String): Array[(Long, Long, String)] = {
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    lines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val provice = fields(6)
      (startNum, endNum, provice)
    }).toArray
  }

  /**
    * 将ip地址转换成long型数字
    */
  def ip2Long(ip: String): Long = {
    val fragment = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragment.length) {
      ipNum = fragment(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def data2MySQL(it: Iterator[(String, Int)]): Unit = {
    //一个迭代器代表一个分区，分区中有多条数据
    //先获得一个JDBC连接
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/yanhuan?characterEncoding=UTF-8", "root", "123456")
    //将数据通过Connection写入到数据库
    val pstm: PreparedStatement = conn.prepareStatement("INSERT INTO access_log VALUES (?, ?)")
    //将分区中的数据一条一条写入到MySQL中
    it.foreach(tp => {
      pstm.setString(1, tp._1)
      pstm.setInt(2, tp._2)
      pstm.executeUpdate()
    })
    //将分区中的数据全部写完之后，在关闭连接
    if(pstm != null) {
      pstm.close()
    }
    if (conn != null) {
      conn.close()
    }
  }

}
