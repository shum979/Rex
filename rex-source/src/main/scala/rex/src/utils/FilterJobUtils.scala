package rex.src.utils

import org.apache.spark.sql.types._
import rex.core.xml.Column

import scala.annotation.tailrec
import scala.reflect.runtime.{universe => ru}
import scala.tools.reflect.ToolBox
import scala.util.matching.Regex

object FilterJobUtils {

  //To convert the column types in dataframes to primitive datatypes
  def getDataTypeMap(schema: StructType): Map[String, String] = {
    val datatype: Seq[(String, String)] = {
      for (x <- schema) yield (x.name, x.dataType match {
        case NullType => "Null"
        case DateType => "Int"
        case TimestampType => "Long"
        case BinaryType => "Array[Byte]"
        case IntegerType => "Int"
        case BooleanType => "Boolean"
        case LongType => "Long"
        case DoubleType => "Double"
        case FloatType => "Float"
        case ShortType => "Short"
        case ByteType => "Byte"
        case StringType => "String"
        case _ => throw new IllegalArgumentException
      })
    }
    datatype.toMap
  }

  //To convert the primitive datatypes to column types in dataframes
  def getStructType(datatype: String): String = {
    datatype match {
      case "Null" => "NullType"
      case "Int" => "IntegerType"
      case "Boolean" => "BooleanType"
      case "Long" => "LongType"
      case "Double" => "DoubleType"
      case "Float" => "FloatType"
      case "Short" => "ShortType"
      case "Byte" => "ByteType"
      case "String" => "StringType"
      case _ => throw new IllegalArgumentException
    }
  }

  //Takes a Sequence of expressions and returns a row object containing the converted expression into dataset
  //specific function
  //eg : if(${age} > 20) ${sal} + 1 else ${sal} + 200 will be converted to
  // if(row.getAs[Int]("age") > 20) row.getAs[Int]("sal") + 1 else row.getAs[Int]("sal") + 200)
  def getExpr(exprStr: Seq[Column], schema: StructType, fieldDataTypeMap: Map[String, String]): String = {
    var exprMap: Map[String, String] = Map[String, String]()
    for (expr <- exprStr) {
      exprMap += (expr.name -> getFilterExpr(expr.expr, fieldDataTypeMap))
    }

    var counter = -1
    var row = "Array("

    for (x <- schema) {
      counter = counter + 1
      val value = exprMap.get(x.name) match {
        case Some(x) => x
        case None => s"row(${counter})"
      }

      if (counter == 0)
        row = row + value
      else
        row = row + "," + value
    }
    row = row + ")"
    row
  }

  //replaces "${\w+\}" in passed String
  // Example :- Input String -- "${age} >10 && ${name} == "c""
  // replaces with -- "datum.row.getAs[Int]("age") > 10 && datum.row.getAs[String]("name") == "c""
  def getFilterExpr(expStr: String, fieldDataTypeMap: Map[String, String]): String = {
    @tailrec
    def replace(regexResultItr: Regex.MatchIterator, fieldRegex: Regex, fieldDataTypeMap: Map[String, String], finalOutput: String): String =
      regexResultItr.hasNext match {
        case true => {
          val fieldRegex(prefix, fieldName, sufix) = regexResultItr.next()
          val filedDatatype = fieldDataTypeMap.getOrElse(fieldName, "Null")
          val targetStr = "row.getAs[" + filedDatatype + "](\"" + fieldName + "\")"
          replace(regexResultItr, fieldRegex, fieldDataTypeMap, finalOutput.replace(prefix + fieldName + sufix, targetStr))
        }
        case false => finalOutput
      }

    val fieldRegex: Regex = new Regex("""(\$\{)(\w+)(\})""")
    val regexResult: Regex.MatchIterator = fieldRegex findAllIn expStr
    replace(regexResult, fieldRegex, fieldDataTypeMap, expStr)
  }

  //Find the return type of the expression based on the function created from generatedFuncStr
  def getReturnType(inputExpr: String, fieldDataTypeMap: Map[String, String]) = {
    val code = generateFuncStr(inputExpr, fieldDataTypeMap)
    val toolBox = ru.runtimeMirror(getClass.getClassLoader).mkToolBox()
    val exp: String = toolBox.typecheck(toolBox.parse(code)).toString()
    val returnType = exp.split("=>")(1).split("=")(0)

    returnType.trim
  }

  //Takes a string expression and generates a scala function to be used later in the getReturnType method
  def generateFuncStr(exp: String, fieldDataTypeMap: Map[String, String]) = {
    val pattern = "\\$\\{(.*?)\\}".r
    //pattern to find the ${columnName} in the expression
    val getColumnsInExp = pattern.findAllIn(exp).matchData.flatMap {
      _.subgroups
    }.toList.distinct

    var resolvedExp = exp

    for (col <- getColumnsInExp)
      resolvedExp = resolvedExp.replace("${" + col + "}".r, col)

    val generatedExp = "val f = (" + getColumnsInExp.map(x => x + ":" + fieldDataTypeMap.get(x).get).mkString(",") + ")" + " => { " + resolvedExp + "}"

    generatedExp
  }
}


