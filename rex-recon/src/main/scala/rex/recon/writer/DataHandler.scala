package rex.recon.writer

import org.apache.spark.sql.Row

/**
  * A trait for handling various kinds of data
  */
trait DataHandler {

  /**
    * This method writes the sequence of rows
    *
    * @param rows
    */
  def writedata(rows: Seq[Row])

  /**
    * This method closes the writer
    */
  def closeWriter()

}
