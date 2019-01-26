package com.yh.spark.learn.ip

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

}
