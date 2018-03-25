package com.kilo.data

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * DataFrame API操作案例
  * Created by kilo on 2018/3/15.
  */
object DataFrameCase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameCase").master("local[2]").getOrCreate()

    val rdd = spark.sparkContext.textFile("G:\\data\\student.data")

    import spark.implicits._
    val studentDF = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    studentDF.show(30, false)
    studentDF.filter("name='' or name ='NULL'").show()
    //    studentDF.filter("SUBSTR(name,0,1)='M'").show()
    //    studentDF.sort("name","id").show()
    //    studentDF.sort(studentDF.col("name").asc,studentDF("id").desc).show()
    //    studentDF.select(studentDF("name").as("student_name")).show()
    //
    //    val studentDF2 = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()
    //
    //    studentDF.join(studentDF2,studentDF.col("id")===studentDF2("id")).show()
    //
//    studentDF.coalesce(1).write.format("json").mode(SaveMode.Overwrite).save("G:/data/output")
    spark.stop()

  }

  case class Student(id: Int, name: String, phone: String, email: String)

}
