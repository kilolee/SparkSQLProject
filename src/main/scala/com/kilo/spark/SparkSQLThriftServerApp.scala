package com.kilo.spark

import java.sql.DriverManager

/**
  * 通过JDBC的方式访问SparkSQL中的数据
  * Created by kilo on 2018/3/15.
  */
object SparkSQLThriftServerApp {
  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val conn = DriverManager.getConnection("jdbc:hive2://192.168.6.159:10000", "root", "123456")
    val pstmt = conn.prepareStatement("select empno,name,sal from emp")
    val rs = pstmt.executeQuery()
    while (rs.next()) {
      println(
        "empno:" + rs.getInt("empno") +
          ",ename:" + rs.getString("ename") +
          ",sal:" + rs.getDouble("sal")
      )
    }

    rs.close()
    pstmt.close()
    conn.close()
  }
}
