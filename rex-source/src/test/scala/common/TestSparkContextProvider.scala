package common

import org.apache.spark.sql.SparkSession

/**
  * Created by Shubham Gupta on 12/6/2017.
  */
trait TestSparkContextProvider {

  lazy val spark = SparkSession
    .builder()
    .appName("Rex-ReconWithAccelerator")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("spark.default.parallelism", "1")

}

