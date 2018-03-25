package com.kilo.log

import com.kilo.log.dao.StatDAO
import com.kilo.log.entity.{DayCityVideoAccessStat, DayVideoAccessStat, DayVideoTrafficsStat}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * TopN统计Spark作业
  * Created by kilo on 2018/3/17.
  */
object TopNStatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TopNStatJob").master("local[2]").config("spark.sql.sources.partitionColumnTypeInference.enabled", "false") getOrCreate()

    val accessDF = spark.read.parquet("file:///G:/data/output/clean")
    accessDF.printSchema()
    accessDF.show(false)


    val day = "20170511"

    import spark.implicits._
    val commonDF = accessDF.filter($"day" === day && $"cmsType" === "video")

    //缓存commonDF
    commonDF.cache()


    //清空数据表
    StatDAO.deleteData(day)

    //最受欢迎的TopN课程
    videoAccessTopNStat(spark, commonDF)

    //按城市进行统计
    cityAccessTopNStat(spark, commonDF)

    //按流量进行统计
    videoTrafficsTopNStat(spark, commonDF)

    //清除缓存
    commonDF.unpersist()

    spark.stop()

  }

  /**
    * 最受欢迎的TopN课程
    *
    * @param spark
    * @param commonDF
    */
  def videoAccessTopNStat(spark: SparkSession, commonDF: DataFrame): Unit = {

    /**
      * 使用DataFrame的方式进行统计
      */
    import spark.implicits._
    val videoAccessTopNDF = commonDF
      .groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)

    /**
      * 使用SQL的方式进行统计
      */
    //    accessDF.createOrReplaceTempView("access_logs")
    //    val videoAccessTopNDF = spark.sql("select day,cmsId,count(1) as times from access_logs " +
    //      "where day='20170511' and cmsType='video' " + "group by day,cmsId order by times desc")

    videoAccessTopNDF.show(false)

    try {
      videoAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")
          list.append(DayVideoAccessStat(day, cmsId, times))
        })
        StatDAO.insertDayVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }


  /**
    * 按照地市进行统计TopN课程
    *
    * @param spark
    * @param commonDF
    */
  def cityAccessTopNStat(spark: SparkSession, commonDF: DataFrame): Unit = {
    import spark.implicits._
    val cityAccessTopNDF = commonDF.groupBy("day", "city", "cmsId")
      .agg(count("cmsId").as("times"))

    //    cityAccessTopNDF.show(false)

    val top3DF = cityAccessTopNDF.select(
      cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
        .orderBy(cityAccessTopNDF("times").desc)).as("times_rank")
    ).filter("times_rank <= 3")

    try {
      top3DF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")
          list.append(DayCityVideoAccessStat(day, cmsId, city, times, timesRank))
        })
        StatDAO.insertDayCityVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 按照流量进行统计
    *
    * @param spark
    * @param commonDF
    */
  def videoTrafficsTopNStat(spark: SparkSession, commonDF: DataFrame): Unit = {
    import spark.implicits._

    val trafficsAccessTopNDF = commonDF.groupBy("day", "cmsId")
      .agg(sum("traffic").as("traffics")).orderBy($"traffics".desc)

    try {
      trafficsAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoTrafficsStat]
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")
          list.append(DayVideoTrafficsStat(day, cmsId, traffics))
        })
        StatDAO.insertDayVideoTrafficsAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }


  }


}
