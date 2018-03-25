package com.kilo.log

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * MySQL操作工具类
  * Created by kilo on 2018/3/18.
  */
object MySQLUtils {
  /**
    * 获取数据库连接
    *
    * @return
    */
  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://192.168.1.112:3306/sparksqlproject", "root", "1234")
  }

  /**
    * 释放数据库连接等资源
    *
    * @param connection
    * @param pstmt
    */
  def release(connection: Connection, pstmt: PreparedStatement) = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(getConnection())
  }
}
