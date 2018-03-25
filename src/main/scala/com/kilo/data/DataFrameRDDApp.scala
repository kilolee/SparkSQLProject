package com.kilo.data

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * DataFrame和RDD的互操作
  * Created by kilo on 2018/3/15.
  */
object DataFrameRDDApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()
//    inferReflection(spark)
    program(spark)

    spark.stop()

  }

  /**
    * RDD==>DataFrame,采用反射的方式
    *
    * @param spark
    */
  def inferReflection(spark: SparkSession) = {

    val rdd = spark.sparkContext.textFile("G:\\data\\infos.txt")
    //注意：需要导入隐式转换
    import spark.implicits._
    val infoDF = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()
    infoDF.printSchema()
    infoDF.show()
    infoDF.filter(infoDF.col("age") > 30).show()

    infoDF.createTempView("infos");
    spark.sql("select * from infos where age > 30").show()

  }

  case class Info(id: Int, name: String, age: Int)

  /**
    * RDD==>DataFrame,采用编程的方式
    *
    * @param spark
    */
  def program(spark: SparkSession) = {
    val rdd = spark.sparkContext.textFile("G:\\data\\infos.txt")
    //创建一个RDDs of Row from the original RDD
    val infoRDD = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).toInt))

    val structType = StructType(Array(StructField("id", IntegerType, true), StructField("name", StringType, true), StructField("age", IntegerType, true)))

    val infoDF = spark.createDataFrame(infoRDD, structType)

    infoDF.printSchema()
    infoDF.show()

  }

}
