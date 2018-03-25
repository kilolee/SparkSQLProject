package com.kilo.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SQLContext的使用；
  * 注意：IDEA是在本地，而测试数据在服务器上，现在在本地进行开发测试；
  * Created by kilo on 2018/3/15.
  */
object SQLContextApp {
  def main(args: Array[String]): Unit = {
    val path = args(0)
    //1、创建相应的Context
    val sparkConf = new SparkConf()
    //在测试或生产环境中，AppName和master是通过脚本指定的
//    sparkConf.setAppName("SQLContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //2、相关的处理：json
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    //3、关闭资源
    sc.stop()
  }
}
