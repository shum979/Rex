package utils

import org.apache.spark.sql.SparkSession

/**
  * Created by Shubham Gupta on 17-Feb-18.
  */
trait TestSparkContextProvider {

  // creating spark sparksession to interact with spark
  lazy val spark = SparkSession
    .builder()
    .appName("Rex-ReconWithAccelerator")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("spark.default.parallelism", "1")
  spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

}
