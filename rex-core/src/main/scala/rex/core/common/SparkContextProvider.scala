package rex.core.common

import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by Shubham Gupta on 12/6/2017.
  * this is central helper utility for accessing spark
  * all configurations are read from config file and are set into spark context
  */
trait SparkContextProvider extends RexConfig {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("Rex-ReconWithAccelerator")
    .master(config.getString("core.spark.master.url"))
    .getOrCreate()

  spark.conf.set("spark.executor.memory", config.getString("core.spark.executor.memory"))
  spark.conf.set("spark.executor.cores", config.getString("core.spark.executor.cores"))
  spark.conf.set("spark.executor.instances", config.getString("core.spark.executor.instances"))

  // getting haldle for spark sql context for application
  val sqlContext: SQLContext = spark.sqlContext
}
