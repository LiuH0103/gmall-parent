package com.atguigu.gmall.realtime.util

import java.util.ResourceBundle

import com.alibaba.fastjson.{JSON, JSONObject}

object ConfigUtil {

    private val bundle: ResourceBundle = ResourceBundle.getBundle("config")
  //  private val condbundle: ResourceBundle = ResourceBundle.getBundle("condition")

/*    def main(args: Array[String]): Unit = {
      //  println(getValueByKey("hive.database"))
        println(getValueByJsonKey("endDate"))
    }*/

    def getValueByKey(key : String): String ={

/*        val stream: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties")

        val properties = new Properties()

        properties.load(stream)
        properties.getProperty(key)*/

        bundle.getString(key)

    }

/*
    def getValueByJsonKey(jsonKey:String):String={
        val str: String = condbundle.getString("condition.params.json")

        val jsonObj: JSONObject = JSON.parseObject(str)
        jsonObj.getString(jsonKey)
    }
*/

}
