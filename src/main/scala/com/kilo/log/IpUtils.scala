package com.kilo.log

import com.ggstar.util.ip.IpHelper

/**
  * IP解析工具类
  * Created by kilo on 2018/3/17.
  */
object IpUtils {

  def getCity(ip:String)={
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    println(getCity("218.75.35.226"))
  }
}
