package com.kilo.externalDataSource

import org.apache.spark.sql.SparkSession

/**
  * Schema 推导
  * Created by kilo on 2018/3/15.
  */
object SchemaInferApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SchemaInferApp").master("local[2]").getOrCreate()

    val df = spark.read.format("json").load("file:///G:/data/people.json")
    df.printSchema()
    df.show()

    spark.stop()
  }

}
