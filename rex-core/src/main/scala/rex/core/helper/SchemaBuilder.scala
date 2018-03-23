package rex.core.helper

import org.apache.spark.sql.types._
import rex.core.xml.{ColumnDataType, SchemaColumnType}

import scala.io.Source
import scala.util.parsing.json._

/**
  * Created by Shubham Gupta on 12/7/2017.
  * this utility class parses schema from json format
  * to spark specific structType
  */
object SchemaBuilder {

  /*method for implicitly converting DataType to string*/
  implicit def dataTypeToString(dt: ColumnDataType): String = dt.toString

  /*this methods reads schema from file and stored in json format and convert into StructType*/
  def buildFromJsonFile(schemaFilePath: String): StructType = {
    var json: String = ""
    for (line <- Source.fromFile(schemaFilePath).getLines()) json += line

    val maybeFields: Option[List[StructField]] = JSON.parseFull(json).map {
      case json: Map[String, List[Map[String, Any]]] =>
        json("fields").map(x => (x("name"), x("type"))).map { case (name, iType) =>
          StructField(name.toString, DataTypes.StringType, true, Metadata.empty)
        }
    }
    StructType(maybeFields.get.toArray)
  }

  /*this utility methods takes schemaColumns from xml and convert it
  * into StructType for spaek*/
  def buildFromXmlColumnList(columns: Seq[SchemaColumnType]): StructType = {
    val fields: Seq[StructField] = columns.map(column =>
      StructField(column.value,
        column.dataType.fold(DataTypes.StringType)(getDataType(_)),
        column.isNullable.getOrElse(true),
        Metadata.empty))
    StructType(fields)
  }

  // method for matching proper data type -- from file to scala
  private def getDataType(inputType: String): DataType = {
    inputType.toLowerCase() match {
      case "stringtype" => DataTypes.StringType
      case "integertype" => DataTypes.IntegerType
      case "longtype" => DataTypes.LongType
      case "floattype" => DataTypes.FloatType
      case "doubletype" => DataTypes.DoubleType
      case unknown => throw new IllegalStateException(s"dataType $unknown is invalid")
    }
  }


}
