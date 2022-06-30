package com.jump.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @ClassName PropertiesUtil
 * @Description
 * @Author Four5prings
 * @Date 2022/6/30 19:16
 */
object PropertiesUtil {

  def load(propertiesName :String):Properties ={
    val pro = new Properties()
    pro.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
      "UTF-8"))
    pro
  }
}
