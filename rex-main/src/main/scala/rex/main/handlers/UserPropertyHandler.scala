package rex.main.handlers

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import rex.core.common.SparkUtils
import rex.core.xml.{PropertiesType, PropertyFilePathType}

import scala.collection.JavaConverters._


/**
  * Created by Shubham Gupta on 28-Feb-18.
  */
object UserPropertyHandler {

  def parseProperties(propertyOption: Option[PropertiesType]) = {

    propertyOption map { properties =>
      val propertyMap = properties.Property.map(property => (property.Key -> property.Value)) toMap
      val propertyMapFromFile: Map[String, String] = properties.PropertyFilePath.flatMap(getPropertyMapFromFile).getOrElse(Map.empty[String, String])

      SparkUtils.addProperties(propertyMap ++ propertyMapFromFile)
    }

  }


  def getPropertyMapFromFile(propertyFilePath: PropertyFilePathType): Option[Map[String, String]] = {
    val filePath = propertyFilePath.value.trim
    val prefix = propertyFilePath.propertyWithPrefix.fold("")(_ + ".")
    val conf = new Configuration
    val fileSystem = FileSystem.get(conf)

    readSafely(fileSystem.open(new Path(filePath)))(_.close()) { inputStream =>
      val bufferedReader = new BufferedReader(new InputStreamReader(inputStream))
      bufferedReader.lines().iterator().asScala.map { line =>
        val array = line.split("=")
        (prefix + array(0).trim -> array(1).trim)
      } toMap
    }
  }


  def readSafely[A, B](resource: => A)(cleanup: A => Unit)(code: A => B): Option[B] = {
    try {
      val r = resource
      try {
        Some(code(r))
      }
      finally {
        cleanup(r)
      }
    } catch {
      case exception: Exception => throw exception
    }
  }

}
