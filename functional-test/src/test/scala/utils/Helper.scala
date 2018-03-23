package utils

import org.apache.spark.sql.Row

import scala.io.Source

/**
  * Created by Shubham Gupta on 21-Feb-18.
  */
object Helper {

  /*utility method for reading data from file and converting it into
  * Array[Row]*/
  def fileToRowArray(fileName: String): Array[Row] = {
    Source.fromFile(fileName).getLines().map { line =>
      Row.fromSeq(line.split(","))
    } toArray
  }
}
