package com.kilo.data

import org.apache.spark.sql.SparkSession

/**
  * DataFrame的基本操作
  * Created by kilo on 2018/3/15.
  */
object DataFrameApp {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()

    //将json文件加载成一个dataFrame
    val peopleDF = spark.read.format("json").load("G:\\data\\people.json")

    //输出dataframe对应的schema信息
    peopleDF.printSchema()

    //输出数据集的前20条记录
    peopleDF.show()

    //查询指定的name列
    peopleDF.select("name").show()

    //查询指定的列，并进行计算
    //select name,age+10 as age2 from table
    peopleDF.select(peopleDF.col("name"), (peopleDF.col("age") + 10).as("age2")).show()

    //select * from table where age > 19
    peopleDF.filter(peopleDF.col("age") > 19).show()

    //select age,count(1) from table group by age
    peopleDF.groupBy("age").count().show()

    spark.stop()
  }
}
