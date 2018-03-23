package rex.src.transform

object TransConfig {

  //refId : reference Id for identify transformation step
  //filterLogic : filter expression
  //filterTyoe : simple for sql style and "complex" for function style
  case class FilterConfig(refId: String, filterLogic: String, filterType: Option[String])

  case class ExprConfig(refId: String, exprList: List[Expr], exprType: Option[String])

  case class Expr(fieldName: String, expr: String, fieldType: Option[String], isNullable: Option[String], isNewField: Option[String])

}
