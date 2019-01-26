package com.yh.spark.learn.sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark排序1
  */
object CustomerSort1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sort1").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val users = Array("laoduan 30 99", "laozhao 28 88", "laohu 31 99", "laowu 27 97", "xiaofang 26 100")

    val lines: RDD[String] = sc.parallelize(users)

    //切分整理数据
    val userRdd: RDD[(String, Int, Int)] = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val value = fields(2).toInt
      //      new User(name, age, value)
      (name, age, value)
    })

    //获取User对象的RDD后的排序对象
    //    val sorted: RDD[User] = userRdd.sortBy(u => u)

    val sorted = userRdd.sortBy(u => new UserCompare(u._2, u._3))

    //根据case类进行排序
    val sortedWithCaseClass = userRdd.sortBy(u => UserCaseCompare(u._2, u._3))

    val result = sorted.collect()

    println(result.toBuffer)

    sc.stop()

  }
}

/**
  * 继承Ordered，可排序
  *
  * @param name
  * @param age
  * @param value
  */
class User(val name: String, val age: Int, val value: Int) extends Ordered[User] with Serializable {
  override def compare(that: User) = {
    if (this.value == that.value) {
      this.age - that.age
    } else {
      -(this.value - that.value)
    }
  }

  override def toString = s"User($name, $age, $value)"
}

class UserCompare(val age: Int, val value: Int) extends Ordered[UserCompare] with Serializable {
  override def compare(that: UserCompare) = {
    if (this.value == that.value) {
      this.age - that.age
    } else {
      -(this.value - that.value)
    }
  }
}

case class UserCaseCompare(val age: Int, val value: Int) extends Ordered[UserCaseCompare] with Serializable {
  override def compare(that: UserCaseCompare) = {
    if (this.value == that.value) {
      this.age - that.age
    } else {
      -(this.value - that.value)
    }
  }
}
