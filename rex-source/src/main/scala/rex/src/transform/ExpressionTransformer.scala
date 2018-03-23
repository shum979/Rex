package rex.src.transform

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import rex.core.common.ManagedDataset
import rex.core.xml.{ExpressionColumnType, TransformationType}
import rex.src.utils.FilterJobUtils._

object ExpressionTransformer extends Transformer {

  private val convStrTofunc =
    (funcStr: String, schema: StructType) => {
      new Function1[Row, Row] with Serializable {

        import scala.reflect.runtime.universe.{Quasiquote, runtimeMirror}
        import scala.tools.reflect.ToolBox

        lazy val mirror = runtimeMirror(getClass.getClassLoader)
        lazy val tb = ToolBox(mirror).mkToolBox()
        lazy val functionWrapper = s"object FunctionWrapper { ${funcStr} }"

        lazy val functionSymbol = tb.define(tb.parse(functionWrapper).asInstanceOf[tb.u.ImplDef])
        lazy val func = tb.eval(q"$functionSymbol.exprFunc _").asInstanceOf[Row => Array[Any]]

        def apply(row: Row): Row =
          new GenericRowWithSchema(func(row), schema)
      }
    }

  override def transform(datasetMap: Map[String, ManagedDataset], config: TransformationType): ManagedDataset = {
    val managedDataset = datasetMap(config.applyondataref)
    val exprConfig = config.Expression.get.ColumnExpression.get
    val oldSchema = managedDataset.schema()
    val newSchema = getNewSchema(oldSchema, exprConfig)
    val exprDetail: ExprDetail = createExprDetail(exprConfig, newSchema)
    applyExpr(managedDataset, exprDetail)
  }

  //Creates the new schema if the any new column is given in the expressions passed from the user
  private def getNewSchema(oldSchema: StructType, exprConfig: ExpressionColumnType): StructType = {
    def getFieldType(fieldType: Option[String]): DataType = {
      fieldType match {
        case Some(x) => x match {
          case "NullType" => NullType
          case "DateType" => DateType
          case "TimestampType" => TimestampType
          case "BinaryType" => BinaryType
          case "IntegerType" => IntegerType
          case "BooleanType" => BooleanType
          case "LongType" => LongType
          case "DoubleType" => DoubleType
          case "FloatType" => FloatType
          case "ShortType" => ShortType
          case "ByteType" => ByteType
          case "StringType" => StringType
          case _ => throw new IllegalArgumentException
        }
        case None => throw new IllegalArgumentException
      }
    }

    var tempSchema = oldSchema
    val oldfieldSeq: Seq[String] = for (y <- oldSchema) yield y.name

    for (expr <- exprConfig.Column) {
      val isNewField: Boolean = expr.isNew.getOrElse(false)

      if (isNewField && !oldfieldSeq.contains(expr.name)) {
        val datatypeMap: Map[String, String] = getDataTypeMap(oldSchema)
        val fieldName = expr.name

        //Identify the return type of the expression if the user doesn't specify the return type of the variable
        val exprType = expr.typeValue match {
          case Some(x) => expr.typeValue
          case None => {
            Some(getStructType(getReturnType(expr.expr, datatypeMap)))
          }
        }

        val fieldType = getFieldType(exprType)

        val isNullable: Boolean = expr.isNullable match {
          case Some(x) => x
          case None => true
          case _ => throw new IllegalArgumentException
        }

        tempSchema = tempSchema.add(StructField(fieldName, fieldType, isNullable))
      }
    }

    tempSchema
  }

  private def createExprDetail(exprConfig: ExpressionColumnType, newSchema: StructType): ExprDetail = {
    val datatypeMap: Map[String, String] = getDataTypeMap(newSchema)
    ExprDetail(getExpr(exprConfig.Column, newSchema, datatypeMap), newSchema)
  }

  private def applyExpr(dataset: ManagedDataset, exprDetail: ExprDetail): ManagedDataset = {
    val funcStr: String = s"def exprFunc(row:org.apache.spark.sql.Row):Array[Any]={ " +
      "import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;" +
      s"${exprDetail.expr}}"

    val func = convStrTofunc(funcStr, exprDetail.newSchema)
    val encoder = RowEncoder(exprDetail.newSchema)

    dataset.map(func, encoder)
  }

  private case class ExprDetail(expr: String, newSchema: StructType)

}
