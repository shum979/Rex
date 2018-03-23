package com.shubham.rex.core.common

/**
  * Created by Shubham Gupta on 07-Feb-18.
  */
object SparkUtils extends SparkContextProvider {

  def addProperties(inMap: Map[String, String]): Unit = {
    inMap foreach { case (k, v) => spark.conf.set(k, v) }
  }

  def getPropertyValue(key: String): Option[String] = {
    Option(spark.conf.get(key))
  }

}
