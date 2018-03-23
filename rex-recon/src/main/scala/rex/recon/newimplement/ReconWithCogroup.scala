package rex.recon.newimplement

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import rex.core.common.ManagedDataset
import rex.core.xml.ReconciliationType
import rex.recon.helper.{ReconConfigDataHolder, ReconUtil}
import rex.src.utils.AppConstants._

/**
  * Created by vrai on 12/5/2017.
  *
  * @param reconConfig
  * ReconWithShuffle class this will do recon on supplied two dataset using cogroup and return reconcilied managed dataset
  */
class ReconWithCogroup(reconConfig: ReconciliationType) extends Serializable {

  /**
    * Create reconConfigdataHolder from supplied xml recon config.Holder will contain all recon config related data
    * ex: comparisionKey, reconkey, schema, duplicateStrategycase class for left and right
    */
  val reconConfigDataHolder: ReconConfigDataHolder = ReconUtil.createReconConfigDataHolder(reconConfig)


  def reconcile(leftDataSet: ManagedDataset, rightDataSet: ManagedDataset): ManagedDataset = {
    /*  Add LHS_/RHS_ as prefix in all columns dataset to avoid ambiguity  */
    val leftRenamedDataset = renameColumnsWithIdentifier(LHS_PRIFIX, leftDataSet)
    val rightRenamedDataset = renameColumnsWithIdentifier(RHS_PRIFIX, rightDataSet)

    val sparkApplicationID = leftDataSet.getSparkJobID()
    val currentdate: Column = current_date()

    /*  reate KeyValueGroupedDataset by doing group by on matching key for left and right both side*/
    val leftGroupedDataSet = leftRenamedDataset.groupByKey { row => row.getValuesMap(reconConfigDataHolder.getLeftMatchingKey).values.mkString("-") }(Encoders.STRING)
    val rightGroupedDataSet = rightRenamedDataset.groupByKey { row => row.getValuesMap(reconConfigDataHolder.getRightMatchingKey).values.mkString("-") }(Encoders.STRING)

    val reconConfigHolder: ReconConfigDataHolder = reconConfigDataHolder.copy(leftDataSetSchema = leftRenamedDataset.schema,
      rightDataSetSchema = rightRenamedDataset.schema)

    /*do cogroup on left and right grouped dataset and return final reconcilied dataset*/
    val reconciledDataSet = new WithCogroupImplementation(reconConfigHolder).doCoGroup(leftGroupedDataSet, rightGroupedDataSet)

    new ManagedDataset(reconciledDataSet.withColumn(RECON_DATE, currentdate).withColumn(JOBID, lit(sparkApplicationID)), Map.empty)
  }


  private def renameColumnsWithIdentifier(identifier: String, dataSet: ManagedDataset) = {
    val renamedColumns = dataSet.columns.map(column => col(column).as(identifier + column))
    dataSet.select(renamedColumns: _*)
  }

}

object ReconWithCogroup {
  def apply(reconConfig: ReconciliationType): ReconWithCogroup = new ReconWithCogroup(reconConfig)
}
