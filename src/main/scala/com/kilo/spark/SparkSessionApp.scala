package com.kilo.spark

import org.apache.spark.sql.SparkSession


/**
  * SparkSession的使用
  * Created by kilo on 2018/3/15.
  */
object SparkSessionApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()

    val people = spark.read.json("G:\\data\\people.json")
    people.show()

    spark.stop()
  }
}
