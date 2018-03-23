package com.shubham.rex.core.helper

/**
  * Created by Shubham Gupta on 06-Feb-18.
  */
object CommonUtils {

  // it takes string like "k1=v1,k2=v2" and translate it into a
  // Map(k1 -> v1, k2 -> v2)
  def buildTagToMap(node: String): Map[String, String] = {
    node.split(",").map(_.split("=")).map(p => p(0) -> p(1)).toMap
  }

  // returns name of user running this application
  def getApplicationUser: String = {
    System.getProperty("user.name")
  }


}
