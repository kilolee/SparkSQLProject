package com.kilo.externalDataSource

import org.apache.spark.sql.SparkSession

/**
  * 使用外部数据源综合查询Hive和MySQL的表数据
  * Created by kilo on 2018/3/15.
  */
object HiveMySQLApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("HiveMySQLApp").master("local[2]").getOrCreate()

    val hiveDF = spark.table("emp")

    val mysqlDF = spark.read.format("jdbc").option("url","jdbc:mysql://192.168.1.134:3306").
      option("driver","com.mysql.jdbc.Driver").
      option("user","root").option("password",1234).option("dbtable","datacube.DEPT").load()

    //JOIN
    val resultDF = hiveDF.join(mysqlDF,hiveDF.col("deptno")===mysqlDF.col("DEPTNO"));
    resultDF.show()

    resultDF.select(hiveDF.col("empno"),hiveDF.col("ename"),mysqlDF.col("deptno"),mysqlDF.col("dname")).show();

    spark.stop()
  }

}
