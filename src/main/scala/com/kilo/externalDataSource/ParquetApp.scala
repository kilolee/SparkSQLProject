package com.kilo.externalDataSource

import org.apache.spark.sql.SparkSession

/**
  * Parquet文件操作
  * Created by kilo on 2018/3/15.
  */
object ParquetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ParquetApp").master("local[2]").getOrCreate()

    val userDF = spark.read.format("parquet").load("file:///root/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet")
    userDF.printSchema()
    userDF.show()

    userDF.select("name", "favorite_color").show()
    userDF.select("name", "favorite_color").write.format("json").save("file:///root/tmp/jsonout")

    spark.stop()
  }
}
