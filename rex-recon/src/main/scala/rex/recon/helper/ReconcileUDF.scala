package rex.recon.helper

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

/**
  * Created by Shubham A Gupta on 12/9/2017.
  */
object ReconcileUDF {

  val udfIsMissing: (Array[String]) => Column = (columns: Array[String]) => {
    val colss: Column = array(columns.map(col(_)): _*)
    val missinUDF = udf(isMissing)
    missinUDF(colss)
  }
  val missingLeftOrRight: (Array[String], Array[String]) => Column = (leftColumns: Array[String], rightColumns: Array[String]) => {
    val left: Column = udfIsMissing(leftColumns).alias("left")
    val right: Column = udfIsMissing(rightColumns).alias("right")
    val leftOrRight: UserDefinedFunction = udf(isMissingInLeftOrRight)
    leftOrRight(left, right)
  }
  val stringtoArrayMultiple = udf((leftcol: String, rightCol: String, leftValue: String, rightValue: String) => {
    customZip(leftcol.split('|').toList, rightCol.split('|').toList, leftValue.split('|').toList, rightValue.split('|').toList)
  })
  private val isMissing: (Seq[Any]) => Boolean = (inputcol: Seq[Any]) => {
    findNull(inputcol)
  }
  private val isMissingInLeftOrRight: (Boolean, Boolean) => String = (leftMissing: Boolean, rightMissing: Boolean) => {
    if (leftMissing)
      "MISSING_ON_LEFT"
    else if (rightMissing)
      "MISSING_ON_RIGHT"
    else
      "RECONCILIED"
  }

  def customZip(list1: List[String], list2: List[String], list3: List[String], list4: List[String]): List[(String, String, String, String)] =
    (list1, list2, list3, list4) match {
      case (Nil, _, _, _) => Nil
      case (_, Nil, _, _) => Nil
      case (head1 :: lst1, head2 :: lst2, Nil, head4 :: lst3) => (head1, head2, "", head4) :: customZip(lst1, lst2, Nil, lst3)
      case (head1 :: lst1, head2 :: lst2, head3 :: lst3, Nil) => (head1, head2, head3, "") :: customZip(lst1, lst2, lst3, Nil)
      case (head1 :: lst1, head2 :: lst2, Nil, Nil) => (head1, head2, "", "") :: customZip(lst1, lst2, Nil, Nil)
      case (head1 :: lst1, head2 :: lst2, head3 :: lst3, head4 :: lst4) => (head1, head2, head3, head4) :: customZip(lst1, lst2, lst3, lst4)
    }

  private def findNull(inputcols: Seq[Any]): Boolean = {
    if (inputcols.isEmpty)
      true
    else if ( {
      val x = inputcols.head; x
    } != null)
      false
    else
      findNull(inputcols.tail)
  }

}
