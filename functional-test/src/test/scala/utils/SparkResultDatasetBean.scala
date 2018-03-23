package utils

import org.apache.spark.sql.Row

/**
  * Created by Shubham Gupta on 21-Feb-18.
  * This is container class to store output of spark job.
  * This is needed to avoid multiple spark calls while calling different "Then" and "And"
  * functions of cucumber
  */
case class SparkResultDatasetBean(rowArray: Array[Row])