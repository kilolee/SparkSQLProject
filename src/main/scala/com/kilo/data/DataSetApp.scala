package com.kilo.data

import org.apache.spark.sql.SparkSession

/**
  * DataSet的使用
  * Created by kilo on 2018/3/15.
  */
object DataSetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataSetApp").master("local[2]").getOrCreate()

    val path = "G:\\\\data\\\\sales.csv"
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    df.show()

    //需要导入隐式转换
    import spark.implicits._
    val ds = df.as[Sales]

    ds.map(line => line.itemId).show()

    spark.stop()
  }

  case class Sales(transactionId: Int, customerId: Int, itemId: Int, amountPaid: Double)

}
