package com.kilo.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 使用Spark完成数据清洗操作
  * Created by kilo on 2018/3/17.
  */
object SparkStatCleanJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("file:///G:/data/access.log")
    //    accessRDD.map(line =>{
    //      val  splits = line.split(" ")
    //      val url = splits(1)
    //    }).take(10).foreach(println)

    val accessDF = spark.createDataFrame(accessRDD.map(line => AccessConvertUtil.parseLog(line)), AccessConvertUtil.struct)
    //    accessDF.printSchema()
    //    accessDF.show(false)

    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save("G:/data/output/clean")
//    accessDF.write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save("G:/data/output/clean2")
//    accessDF.write.format("parquet").mode(SaveMode.Overwrite).save("G:/data/output/clean3")
    spark.stop()
  }
}
