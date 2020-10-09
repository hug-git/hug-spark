package com.atguigu.streaming.practice.utils

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {

  def load(propertiesName: String): Properties = {

    val prop = new Properties()
    prop.load(new InputStreamReader(ClassLoader.getSystemResourceAsStream(propertiesName), "UTF-8"))
    prop
  }

}
