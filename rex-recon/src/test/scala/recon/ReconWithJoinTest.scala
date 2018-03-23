package recon

import common.TestSparkContextProvider
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, RowFactory}
import org.scalatest.{Matchers, WordSpec}
import rex.core.common.ManagedDataset
import rex.core.xml.ReconciliationType
import rex.recon.implement.ReconWithJoin

import scala.collection.mutable.ListBuffer
import scala.xml.XML

/**
  * Created by vrai on 1/22/2018.
  */
class ReconWithJoinTest extends WordSpec with Matchers with TestSparkContextProvider {


  "recon.Reconciler" should {
    "reconcile two given dataset with all combination" in {

      spark.sparkContext.setLogLevel("ERROR")
      val leftFilePath: String = this.getClass.getResource("/test1/sampleTestFile.txt").getPath
      val rightFilePath: String = this.getClass.getResource("/test1/sampleTestFile2.txt").getPath

      val ds1: DataFrame = spark.read.format("csv").option("header", "true").load(leftFilePath)
      val leftDS = ManagedDataset(ds1, Map("" -> ""))

      val ds2: DataFrame = spark.read.format("csv").option("header", "true").load(rightFilePath)
      val rightDS = ManagedDataset(ds2, Map("" -> ""))


      val xmlRecon = "<Reconciliation name=\"ReconFlow\" applyondataref=\"Source_Ingest,Target_Ingest\" >" +
        " <Source>Source_Ingest</Source>" +
        "<Target>Target_Ingest</Target>" +
        "<ComparisonKey>" +
        "<Column>name=name</Column>" +
        "<Column>risk=risk</Column>" +
        "</ComparisonKey>" +
        "<NumericColumns>" +
        "<Column>amount=amount</Column>" +
        "<Column>age=age</Column>" +
        "</NumericColumns>" +
        "<DeDuplicationStrategy side=\"source\">" +
        "<DeDupColumns>" +
        "<Column>name</Column>" +
        "<Column>risk</Column>" +
        "</DeDupColumns>" +
        "<Strategy columns=\"amount\">max</Strategy>" +
        "</DeDuplicationStrategy>" +
        "<DeDuplicationStrategy side=\"target\">" +
        "<DeDupColumns>" +
        "<Column>name</Column>" +
        "<Column>risk</Column>" +
        "</DeDupColumns>" +
        "<Strategy columns=\"amount\">max</Strategy>" +
        "</DeDuplicationStrategy>" +
        "</Reconciliation>"


      val reconciliationType = scalaxb.fromXML[ReconciliationType](XML.loadString(xmlRecon))

      val reconResult = ReconWithJoin(reconciliationType).reconcile(leftDS, rightDS)

      val recordLevelDS = reconResult._1
      val fieldLevelDS = reconResult._2
      recordLevelDS.where("rowstatus=='MATCH'").show()
      assert(recordLevelDS.where("rowstatus=='MATCH'").count == 2)
      assert(recordLevelDS.where("rowstatus=='BREAK'").count == 1)
      assert(recordLevelDS.where("rowstatus=='MISSING_ON_LEFT'").count == 1)
      assert(recordLevelDS.where("rowstatus=='MISSING_ON_RIGHT'").count == 1)
      assert(fieldLevelDS.where("leftcolumnname=='lhs_amount'").count == 1)
      assert(fieldLevelDS.where("lhs_name=='ravi'").count == 2)
      assert(recordLevelDS.count === 5)
      assert(fieldLevelDS.count === 2)
    }
  }


  "reconciler " should {
    "reconciled two dataset with all matching record" in {

      spark.sparkContext.setLogLevel("ERROR")

      val schema = StructType(
        StructField("name", StringType, false) ::
          StructField("rollnumber", StringType, false) ::
          StructField("id", StringType, false) ::
          StructField("amount", StringType, false) ::
          StructField("course", StringType, false) ::
          Nil
      )

      var recordStatus = new ListBuffer[String]()
      recordStatus += "vishal#123#10#123.12#ENG"
      recordStatus += "rai#120#20#123#BSC"

      val rddRow: RDD[Row] = spark.sparkContext.parallelize(recordStatus.map(x => RowFactory.create(x.split("#").toSeq: _*)))
      val ds1: DataFrame = spark.createDataFrame(rddRow, schema)

      var recordStatus1 = new ListBuffer[String]()
      recordStatus1 += "vishal#123#10#123.12#ENG"
      recordStatus1 += "rai#120#10#123# "
      recordStatus1 += "amit#121# # # "
      recordStatus1 += " # # # # "

      val rddRow1: RDD[Row] = spark.sparkContext.parallelize(recordStatus1.map { x => RowFactory.create(x.split("#").toSeq: _*) })
      val ds2: DataFrame = spark.createDataFrame(rddRow1, schema)

      val xmlRecon = "<Reconciliation name=\"ReconFlow\" applyondataref=\"Source_Ingest,Target_Ingest\" >" +
        " <Source>Source_Ingest</Source>" +
        "<Target>Target_Ingest</Target>" +
        "<ComparisonKey>" +
        "<Column>name=name</Column>" +
        "<Column>rollnumber=rollnumber</Column>" +
        "</ComparisonKey>" +
        "<NumericColumns>" +
        "<Column>id=id</Column>" +
        "<Column>amount=amount</Column>" +
        "</NumericColumns>" +
        "<ReconcileColumns>" +
        "<Column>course=course</Column>" +
        "</ReconcileColumns>" +
        "<DeDuplicationStrategy side=\"source\">" +
        "<DeDupColumns>" +
        "<Column>name</Column>" +
        "<Column>rollnumber</Column>" +
        "</DeDupColumns>" +
        "<Strategy columns=\"amount\">min</Strategy>" +
        "</DeDuplicationStrategy>" +
        "<DeDuplicationStrategy side=\"target\">" +
        "<DeDupColumns>" +
        "<Column>name</Column>" +
        "<Column>rollnumber</Column>" +
        "</DeDupColumns>" +
        "<Strategy columns=\"amount\">min</Strategy>" +
        "</DeDuplicationStrategy>" +
        "</Reconciliation>"

      val reconciliationType = scalaxb.fromXML[ReconciliationType](XML.loadString(xmlRecon))
      val reconResult = ReconWithJoin(reconciliationType).reconcile(new ManagedDataset(ds1, Map("" -> "")), new ManagedDataset(ds2, Map("" -> "")))

      assert(reconResult._1.count === 4)
      assert(reconResult._2.count === 2)
    }
  }

  "reconciler " should {
    "find diff in row for given columns" in {

      val schema = StructType(
        StructField("name", StringType, false) ::
          StructField("rollnumber", StringType, false) ::
          StructField("id", StringType, false) ::
          StructField("amount", StringType, false) ::
          StructField("course", StringType, false) ::
          Nil
      )

      var recordStatus = new ListBuffer[String]()
      recordStatus += "vishal#123#10#123.12#ENG"
      recordStatus += "rai#120#20#123#BSC"

      val rddRow: RDD[Row] = spark.sparkContext.parallelize(recordStatus.map(x => RowFactory.create(x.split("#").toSeq: _*)))
      val ds1: DataFrame = spark.createDataFrame(rddRow, schema)
      val firstRow = ds1.first()

      val xmlRecon = "<Reconciliation name=\"ReconFlow\" applyondataref=\"Source_Ingest,Target_Ingest\" >" +
        " <Source>Source_Ingest</Source>" +
        "<Target>Target_Ingest</Target>" +
        "<ComparisonKey>" +
        "<Column>name=name</Column>" +
        "<Column>rollnumber=rollnumber</Column>" +
        "</ComparisonKey>" +
        "<NumericColumns>" +
        "<Column>rollnumber=amount</Column>" +
        "</NumericColumns>" +
        "<ReconcileColumns>" +
        "<Column>course=course</Column>" +
        "</ReconcileColumns>" +
        "<DeDuplicationStrategy side=\"source\">" +
        "<DeDupColumns>" +
        "<Column>amount</Column>" +
        "</DeDupColumns>" +
        "<Strategy>min</Strategy>" +
        "</DeDuplicationStrategy>" +
        "<DeDuplicationStrategy side=\"target\">" +
        "<DeDupColumns>" +
        "<Column>amount</Column>" +
        "</DeDupColumns>" +
        "<Strategy>min</Strategy>" +
        "</DeDuplicationStrategy>" +
        "</Reconciliation>"

      val reconciliationType = scalaxb.fromXML[ReconciliationType](XML.loadString(xmlRecon))
      val matchStatus: Boolean = ReconWithJoin(reconciliationType).findDiffInFieldFromRow(firstRow, "rollnumber=amount")
      assert(matchStatus === false)
    }
  }

  "reconciler " should {
    "throw assestion error if no proper keys provided" in {

      val schema = StructType(
        StructField("name", StringType, false) ::
          StructField("rollnumber", StringType, false) ::
          StructField("id", StringType, false) ::
          StructField("amount", StringType, false) ::
          StructField("course", StringType, false) ::
          Nil
      )

      var recordStatus = new ListBuffer[String]()
      recordStatus += "vishal#123#10#123.12#ENG"
      recordStatus += "rai#120#20#123#BSC"

      val rddRow: RDD[Row] = spark.sparkContext.parallelize(recordStatus.map(x => RowFactory.create(x.split("#").toSeq: _*)))
      val ds1: DataFrame = spark.createDataFrame(rddRow, schema)
      val firstRow = ds1.first()

      val xmlRecon = "<Reconciliation name=\"ReconFlow\" applyondataref=\"Source_Ingest,Target_Ingest\" >" +
        " <Source>Source_Ingest</Source>" +
        "<Target>Target_Ingest</Target>" +
        "<ComparisonKey>" +
        "<Column>name=name</Column>" +
        "<Column>rollnumber</Column>" +
        "</ComparisonKey>" +
        "<NumericColumns>" +
        "<Column>rollnumber=amount</Column>" +
        "</NumericColumns>" +
        "<ReconcileColumns>" +
        "<Column>course=course</Column>" +
        "</ReconcileColumns>" +
        "<DeDuplicationStrategy side=\"source\">" +
        "<DeDupColumns>" +
        "<Column>amount</Column>" +
        "</DeDupColumns>" +
        "<Strategy>min</Strategy>" +
        "</DeDuplicationStrategy>" +
        "<DeDuplicationStrategy side=\"target\">" +
        "<DeDupColumns>" +
        "<Column>amount</Column>" +
        "</DeDupColumns>" +
        "<Strategy>min</Strategy>" +
        "</DeDuplicationStrategy>" +
        "</Reconciliation>"

      val reconciliationType = scalaxb.fromXML[ReconciliationType](XML.loadString(xmlRecon))
      val caught = intercept[AssertionError] {
        val matchStatus: Boolean = ReconWithJoin(reconciliationType).findDiffInFieldFromRow(firstRow, "rollnumber=amount")
      }
      assert(caught.getMessage.startsWith("rollnumber"))
    }
  }

  "recon.Reconciler" should {
    "reconcile two given dataset with numeric columns" in {

      spark.sparkContext.setLogLevel("ERROR")
      val leftFilePath: String = this.getClass.getResource("/test2/sampleTestFile.txt").getPath
      val rightFilePath: String = this.getClass.getResource("/test2/sampleTestFile2.txt").getPath

      val schema = StructType(
        StructField("name", StringType, false) ::
          StructField("age", IntegerType, false) ::
          StructField("risk", StringType, false) ::
          StructField("amount", DoubleType, false) ::
          Nil
      )


      val ds1: DataFrame = spark.read.format("csv").option("header", "true").schema(schema).load(leftFilePath)
      val leftDS = new ManagedDataset(ds1, Map("" -> ""))

      val ds2: DataFrame = spark.read.format("csv").option("header", "true").schema(schema).load(rightFilePath)
      val rightDS = new ManagedDataset(ds2, Map("" -> ""))

      val xmlRecon = "<Reconciliation name=\"ReconFlow\" applyondataref=\"Source_Ingest,Target_Ingest\" >" +
        " <Source>Source_Ingest</Source>" +
        "<Target>Target_Ingest</Target>" +
        "<ComparisonKey>" +
        "<Column>name=name</Column>" +
        "<Column>risk=risk</Column>" +
        "</ComparisonKey>" +
        "<NumericColumns>" +
        "<Column>amount=amount</Column>" +
        "<Column>age=age</Column>" +
        "</NumericColumns>" +
        "<DeDuplicationStrategy side=\"source\">" +
        "<DeDupColumns>" +
        "<Column>amount</Column>" +
        "</DeDupColumns>" +
        "<Strategy>min</Strategy>" +
        "</DeDuplicationStrategy>" +
        "<DeDuplicationStrategy side=\"target\">" +
        "<DeDupColumns>" +
        "<Column>amount</Column>" +
        "</DeDupColumns>" +
        "<Strategy>min</Strategy>" +
        "</DeDuplicationStrategy>" +
        "</Reconciliation>"

      val reconciliationType: ReconciliationType = scalaxb.fromXML[ReconciliationType](XML.loadString(xmlRecon))

      val reconResult = ReconWithJoin(reconciliationType).reconcile(leftDS, rightDS)

      val recordLevelDS = reconResult._1
      val fieldLevelDS = reconResult._2

      assert(recordLevelDS.where(s"rowstatus=='MATCH'").count == 1)
      assert(recordLevelDS.where(s"rowstatus=='BREAK'").count == 3)
      assert(fieldLevelDS.where(s"leftcolumnname=='lhs_amount'").count == 3)
      assert(recordLevelDS.count === 4)
      assert(fieldLevelDS.count === 5)
    }
  }

}
