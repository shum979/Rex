package rex.recon.helper

import org.apache.spark.sql.DataFrame
import rex.core.common.ManagedDataset
import rex.recon.helper.ReconcileUDF._
import rex.src.utils.AppConstants._

/**
  * Created by visrai on 2/26/2018.
  */
object ReconPostProcessingReports {

  /*
  Post processing method used to create post processing reports
  1. Create break_result records by extracting only BREAK records from master table and flattern multiple field level break into multiple rows
   2. Create summary_result for summarisied view of records with count
   */
  def createBreakRecordsDataSet(reconResult: ManagedDataset): Map[String, ManagedDataset] = {

    import org.apache.spark.sql.functions._

    val breakDataSet: ManagedDataset = reconResult.where(s"$ROW_STATUS=='BREAK'")

    val multiRowBreakedDataSet: ManagedDataset = breakDataSet.withColumn("arraycol", explode(stringtoArrayMultiple(col(LEFTCOLUMNNAME), col(RIGHTCOLUMNNAME), col(LEFTCOLUMNVALUE), col(RIGHTCOLUMNVALUE))))
      .withColumn(LEFTCOLUMNNAME, col("arraycol").getItem("_1"))
      .withColumn(RIGHTCOLUMNNAME, col("arraycol").getItem("_2"))
      .withColumn(LEFTCOLUMNVALUE, col("arraycol").getItem("_3"))
      .withColumn(RIGHTCOLUMNVALUE, col("arraycol").getItem("_4"))
      .drop(col("arraycol"))

    val summaryResult: DataFrame = reconResult.groupBy(List(col(ROW_STATUS), col(JOBID), col(RECON_DATE))).count().coalesce(1)

    Map((BREAK_RESULT -> multiRowBreakedDataSet), (SUMMARY_RESULT -> new ManagedDataset(summaryResult, Map.empty)))
  }

}
