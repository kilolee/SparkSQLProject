package com.kilo.spark

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * HiveContext的使用
  * 使用时需要通过--jars 把mysql的驱动传递到classpath
  * Created by kilo on 2018/3/15.
  */
object HiveContextApp {

  def main(args: Array[String]): Unit = {
    //1、创建相应的Context
    val sparkConf = new SparkConf()
    //在测试或生产中，AppName和Master是通过脚本进行指定的
//    sparkConf.setAppName("HiveContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    //2、相关的处理：json
    hiveContext.table("emp").show()

    //3、关闭资源
    sc.stop()

  }

}
