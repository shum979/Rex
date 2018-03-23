package rex.src.transform

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import rex.core.common.ManagedDataset
import rex.src.transform.TransConfig.FilterConfig
import rex.src.utils.FilterJobUtils._

object TransFilter {

  private val convStrTofunc = (funcStr: String) => {
    new Function1[Row, Boolean] with Serializable {

      import scala.reflect.runtime.universe.{Quasiquote, runtimeMirror}
      import scala.tools.reflect.ToolBox

      lazy val mirror = runtimeMirror(getClass.getClassLoader)
      lazy val tb = ToolBox(mirror).mkToolBox()
      lazy val functionWrapper = s"object FunctionWrapper { ${funcStr} }"
      lazy val functionSymbol = tb.define(tb.parse(functionWrapper).asInstanceOf[tb.u.ImplDef])
      lazy val func = tb.eval(q"$functionSymbol.filter _").asInstanceOf[Row => Boolean]

      def apply(row: Row): Boolean = func(row)
    }
  }
  private var schema: StructType = null

  implicit class FilterRDD(dataset: ManagedDataset) {
    def filterData(filterConfig: FilterConfig): ManagedDataset = {
      schema = dataset.schema
      val filterCondition: FilterCondition = createFilterCondition(filterConfig)
      filterRecords(dataset, filterCondition)
    }
  }

  private def createFilterCondition(filterConfig: FilterConfig): FilterCondition = {
    val datatypeMap: Map[String, String] = getDataTypeMap(schema)
    FilterCondition(filterConfig.refId, getFilterExpr(filterConfig.filterLogic, datatypeMap), filterConfig.filterType)
  }

  private def filterRecords(datset: ManagedDataset, filterCondition: FilterCondition): ManagedDataset = {
    val funcStr: String = s"def filter(row:org.apache.spark.sql.Row):Boolean={ ${filterCondition.filterLogic} }"
    val filterfunc = convStrTofunc(funcStr)
    datset.filter(filterfunc)
  }

  private case class FilterCondition(refId: String, filterLogic: String, filterType: Option[String])

}
