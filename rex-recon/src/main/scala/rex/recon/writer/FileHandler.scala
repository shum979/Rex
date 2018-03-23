package rex.recon.writer

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.Row

/**
  * Implementation of DataHandler for files
  *
  * @param fileWriter
  */
class FileHandler(fileWriter: PrintWriter) extends DataHandler {

  /**
    * Writes rows with lines terminated by '\n'
    *
    * @param rows
    */
  override def writedata(rows: Seq[Row]): Unit = {

    try {
      fileWriter.write(rows.mkString("\n"))
      fileWriter.write("\n")
    }
    catch {
      case ex: Exception => ex.printStackTrace()
    }
    finally {
      //fileWriter.flush()
      //fileWriter.close()
    }

  }

  /**
    * This method flushes and closes the file writer
    */
  override def closeWriter(): Unit = {
    fileWriter.flush()
    fileWriter.close()
  }
}

/**
  * A class to build the File Handler using
  *
  * @param location
  * @param fileName
  */
class FileHandlerBuilder(location: String = null, fileName: String = null) {

  def withLocation(location: String) = {
    copy(location = location)
  }

  def copy(location: String = this.location, fileName: String = fileName) =
    new FileHandlerBuilder(location, fileName)

  def withFileName(fileName: String) = {
    copy(fileName = fileName)
  }

  def build(): FileHandler = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val currentTimeMillSecond: Long = System.currentTimeMillis()
    val output: FSDataOutputStream = fs.create(new Path(location + "/" + currentTimeMillSecond + fileName))
    val writer: PrintWriter = new PrintWriter(output)
    new FileHandler(writer)
  }


}

