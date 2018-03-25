package com.kilo.externalDataSource

import org.apache.spark.sql.SparkSession

/**
  * Shcema 合并
  * Created by kilo on 2018/3/15.
  */
object SchemaMergeApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SchemaMergeApp").master("local[2]").getOrCreate()

    import spark.implicits._
    val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
    squaresDF.write.parquet("/root/data/test_table/key=1")

    val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
    cubesDF.write.parquet("/root/data/test_table/key=2")

    val mergedDF = spark.read.option("mergeSchema", "true").parquet("/root/data/test_table")

    mergedDF.printSchema()
    mergedDF.show()

  }
}
