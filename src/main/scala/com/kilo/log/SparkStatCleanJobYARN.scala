package com.kilo.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 使用Spark完成数据清洗操作：运行在YARN之上
  * Created by kilo on 2018/3/17.
  */
object SparkStatCleanJobYARN {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage:SparkStatCleanJobYARN <inputPath> <outputPath>")
      System.exit(1)
    }

    val Array(inputPath, outputPath) = args

    val spark = SparkSession.builder().getOrCreate()

    val accessRDD = spark.sparkContext.textFile(inputPath)

    //RDD ==> DF
    val accessDF = spark.createDataFrame(accessRDD.map(line => AccessConvertUtil.parseLog(line)), AccessConvertUtil.struct)

    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save(outputPath)

    spark.stop()
  }
}
