package com.kilo.log

import org.apache.spark.sql.SparkSession

/**
  * 第一步：清洗数据，抽取需要的指定列的数据
  * Created by kilo on 2018/3/16.
  */
object SparkStatFormatJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()

    val access = spark.sparkContext.textFile("file:///G:/data/10000_access.log")
//        access.take(10).foreach(println)
    access.map(line => {
      val splits = line.split(" ")
      val ip = splits(0)
      val time = splits(3) + " " + splits(4)
      val url = splits(11).replace("\"", "")
      val traffic = splits(9)
      DateUtils.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
    }).saveAsTextFile("G:/data/output/format")

    spark.stop()
  }
}
