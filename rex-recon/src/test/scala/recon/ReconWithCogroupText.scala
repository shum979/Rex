package recon

import common.TestSparkContextProvider
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, RowFactory}
import org.scalatest.{Matchers, WordSpec}
import rex.core.common.ManagedDataset
import rex.core.xml.ReconciliationType
import rex.recon.Reconciler

import scala.collection.mutable.ListBuffer
import scala.xml.XML

/**
  * Created by visrai on 2/22/2018.
  */
class ReconWithCogroupText extends WordSpec with Matchers with TestSparkContextProvider {

  "recon.Reconciler" ignore {
    "reconcile two given dataset with sample data" in {

      spark.sparkContext.setLogLevel("ERROR")
      val leftFilePath: String = this.getClass.getResource("/test3/outFin_csv_5records").getPath
      val rightFilePath: String = this.getClass.getResource("/test3/outGS_csv_5records").getPath

      val ds1: DataFrame = spark.read.format("csv").option("header", "true").load(leftFilePath)
      val leftDS = ManagedDataset(ds1, Map("" -> ""))

      val ds2: DataFrame = spark.read.format("csv").option("header", "true").load(rightFilePath)
      val rightDS = ManagedDataset(ds2, Map("" -> ""))


      val xmlRecon = "<Reconciliation name=\"ReconFlow\" applyondataref=\"Source_Ingest,Target_Ingest\" >" +
        " <Source>Source_Ingest</Source>" +
        "<Target>Target_Ingest</Target>" +
        "<ComparisonKey>" +
        "<Column>Firm_ROE_ID=firm_ROE_ID</Column>" +
        "</ComparisonKey>" +
        "<NumericColumns>" +
        "<Column>Shares_Quantity=shares_Quantity</Column>" +
        "</NumericColumns>" +
        "<ReconcileColumns>" +
        "<Column>Order_Event_Type_code=order_Event_Type_Code</Column>" +
        "<Column>Firm_ROE_ID=firm_ROE_ID</Column>" +
        "<Column>_Order_ReceivingFirm_MPID_=order_Receiving_Firm_MPID</Column>" +
        "<Column>_Order_Receiving_Firm_Order_Received_Date_=order_Receiving_Firm_Order_Received_Date</Column>" +
        "<Column>_Order_Receiving_Firm_Order_ID_=order_Receiving_Firm_Order_ID</Column>" +
        "<Column>_Order_Received_Timestamp_=order_Received_Timestamp</Column>" +
        "<Column>Issue_Symbol_ID=issue_Symbol_ID</Column>" +
        "<Column>Buy_Sell_Code=buy_Sell_Code</Column>" +
        "<Column>Limit_Price=limit_Price</Column>" +
        "<Column>_Time_in_Force_Code_=time_in_Force_Code</Column>" +
        "<Column>Expiration_Date=expiration_Date</Column>" +
        "<Column>_Trading_Session_Code_=trading_Session_Code</Column>" +
        "<Column>_Account_Type_Code_=account_Type_Code</Column>" +
        "<Column>_Program_Trading_Code_=program_Trading_Code</Column>" +
        "<Column>Arbitrage_Code=arbitrage_Code</Column>" +
        "<Column>_Sent_To_Routed_Order_ID_=sent_to_Routed_Order_ID</Column>" +
        "<Column>_Sent_to_Firm_MPID_=sent_to_Firm_MPID</Column>" +
        "<Column>_Order_Sent_Timestamp_=order_Sent_Timestamp</Column>" +
        "<Column>_Routed_Shares_Quantity_=routed_Shares_Quantity</Column>" +
        "<Column>_Destination_Code_=destination_Code</Column>" +
        "<Column>_Routed_Order_Type_Indicator_=routed_Order_Type_Indicator</Column>" +
        "<Column>Route_Price=route_Price</Column>" +
        "<Column>_Order_Origination_Code_=order_Origination_Code</Column>" +
        "</ReconcileColumns>" +
        "<DeDuplicationStrategy side=\"source\">" +
        "<DeDupColumns>" +
        "<Column>Firm_ROE_ID</Column>" +
        "</DeDupColumns>" +
        "<Strategy columns=\"Shares_Quantity\">max</Strategy>" +
        "</DeDuplicationStrategy>" +
        "<DeDuplicationStrategy side=\"target\">" +
        "<DeDupColumns>" +
        "<Column>firm_ROE_ID</Column>" +
        "</DeDupColumns>" +
        "<Strategy columns=\"shares_Quantity\">max</Strategy>" +
        "</DeDuplicationStrategy>" +
        "</Reconciliation>"


      val reconciliationType = scalaxb.fromXML[ReconciliationType](XML.loadString(xmlRecon))

      val reconResult: ManagedDataset = Reconciler(reconciliationType).recon(leftDS, rightDS)

      assert(reconResult.where(s"rowstatus=='MATCH'").count == 4)
      assert(reconResult.where(s"rowstatus=='BREAK'").count == 1)
      assert(reconResult.where(s"rowstatus=='MISSING_ON_RIGHT'").count == 1)

    }
  }

  "Reconciler Test1" should {
    "reconcile two dataset with all combination" in {

      spark.sparkContext.setLogLevel("ERROR")
      val leftFilePath: String = this.getClass.getResource("/test1/sampleTestFile.txt").getPath
      val rightFilePath: String = this.getClass.getResource("/test1/sampleTestFile2.txt").getPath

      val ds1: DataFrame = spark.read.format("csv").option("header", "true").load(leftFilePath)
      val leftDS = new ManagedDataset(ds1, Map("" -> ""))

      val ds2: DataFrame = spark.read.format("csv").option("header", "true").load(rightFilePath)
      val rightDS = new ManagedDataset(ds2, Map("" -> ""))


      val xmlRecon =
        """
          |<Reconciliation name="ReconFlow" applyondataref="Source_Ingest,Target_Ingest" >
          |	<Source>Source_Ingest</Source>
          |	<Target>Target_Ingest</Target>
          |	<ComparisonKey>
          |		<Column>name=name</Column>
          |		<Column>risk=risk</Column>
          |	</ComparisonKey>
          |	<NumericColumns>
          |		<Column>amount=amount</Column>
          |		<Column>age=age</Column>
          |	</NumericColumns>
          |	<DeDuplicationStrategy side="source">
          |		<DeDupColumns>
          |			<Column>name</Column>
          |			<Column>risk</Column>
          |		</DeDupColumns>
          |		<Strategy columns="amount">max</Strategy>
          |	</DeDuplicationStrategy>
          |	<DeDuplicationStrategy side="target">
          |		<DeDupColumns>
          |			<Column>name</Column>
          |			<Column>risk</Column>
          |		</DeDupColumns>
          |		<Strategy columns="amount">max</Strategy>
          |	</DeDuplicationStrategy>
          |</Reconciliation>
        """.stripMargin


      val reconciliationType = scalaxb.fromXML[ReconciliationType](XML.loadString(xmlRecon))

      val reconResult: ManagedDataset = Reconciler(reconciliationType).recon(leftDS, rightDS)

      assert(reconResult.where(s"rowstatus=='MATCH'").count == 2)
      assert(reconResult.where(s"rowstatus=='BREAK'").count == 1)
      assert(reconResult.where(s"rowstatus=='MISSING_ON_LEFT'").count == 1)
    }
  }

  "Reconciler Test2" should {
    "reconcile two datasets having duplicate records" in {

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
      recordStatus += "vishal#201#10#123.12#ENG"
      recordStatus += "vishal#123#10#123.12#ENGG"
      recordStatus += "vishal#123#10#123.12#ENGG"
      recordStatus += "vishal#123#10#123.12#ENGG"
      recordStatus += "vishal#123#10#123.12#ENGG"
      recordStatus += "rai#120#20#123#BSC"

      val rddRow: RDD[Row] = spark.sparkContext.parallelize(recordStatus.map(x => RowFactory.create(x.split("#").toSeq: _*)))
      val ds1: DataFrame = spark.createDataFrame(rddRow, schema)

      var recordStatus1 = new ListBuffer[String]()
      recordStatus1 += "vishal#201#10#123.12#ENG"
      recordStatus1 += "vishal#123#10#123.12#ENGG"
      recordStatus1 += "vishal#123#10#123.12#ENGG"
      recordStatus1 += "rai#120#10#123# "


      val rddRow1: RDD[Row] = spark.sparkContext.parallelize(recordStatus1.map { x => RowFactory.create(x.split("#").toSeq: _*) })
      val ds2: DataFrame = spark.createDataFrame(rddRow1, schema)

      val xmlRecon =
        """<Reconciliation name="ReconFlow" applyondataref="Source_Ingest,Target_Ingest" >
          |	<Source>Source_Ingest</Source>
          |	<Target>Target_Ingest</Target>
          |	<ComparisonKey>
          |		<Column>name=name</Column>
          |		<Column>rollnumber=rollnumber</Column>
          |	</ComparisonKey>
          |	<ReconcileColumns>
          |		<Column>course=course</Column>
          |	</ReconcileColumns>
          |	<DeDuplicationStrategy side="source">
          |		<DeDupColumns>
          |			<Column>name</Column>
          |			<Column>rollnumber</Column>
          |		</DeDupColumns>
          |		<Strategy columns="amount">min</Strategy>
          |	</DeDuplicationStrategy>
          |	<DeDuplicationStrategy side="target">
          |		<DeDupColumns>
          |			<Column>name</Column>
          |			<Column>rollnumber</Column>
          |		</DeDupColumns>
          |		<Strategy columns="amount">min</Strategy>
          |	</DeDuplicationStrategy>
          |</Reconciliation>
          |""".stripMargin

      val reconciliationType = scalaxb.fromXML[ReconciliationType](XML.loadString(xmlRecon))
      val reconResult: ManagedDataset = Reconciler(reconciliationType).recon(new ManagedDataset(ds1, Map("" -> "")), new ManagedDataset(ds2, Map("" -> "")))

      assert(reconResult.where(s"rowstatus=='MATCH'").count == 2)
      assert(reconResult.where(s"rowstatus=='BREAK'").count == 1)
      assert(reconResult.where(s"rowstatus=='DUPLICATE_ON_RIGHT'").count == 1)
      assert(reconResult.where(s"rowstatus=='DUPLICATE_ON_LEFT'").count == 3)
    }
  }

  "Reconciler Test3" should {
    "reconciled two dataset with duplicate records and all possible combination" in {

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
      recordStatus += "vishal#123#10#123.32#ENGG"
      recordStatus += "rai#120#20#123#BSC"
      recordStatus += "amit#919#50#8712#BCOM"
      recordStatus += "amit#919#50#8712#BCOM"
      recordStatus += "vishal001#19872#50#8712#BCOM"

      val rddRow: RDD[Row] = spark.sparkContext.parallelize(recordStatus.map(x => RowFactory.create(x.split("#").toSeq: _*)))
      val ds1: DataFrame = spark.createDataFrame(rddRow, schema)

      var recordStatus1 = new ListBuffer[String]()
      recordStatus1 += "vishal#123#10#123.12#ENG"
      recordStatus1 += "vishal#123#10#123.32#ENGG"
      recordStatus1 += "rai#120#10#123# "


      val rddRow1: RDD[Row] = spark.sparkContext.parallelize(recordStatus1.map { x => RowFactory.create(x.split("#").toSeq: _*) })
      val ds2: DataFrame = spark.createDataFrame(rddRow1, schema)

      val xmlRecon = """<Reconciliation name="ReconFlow" applyondataref="Source_Ingest,Target_Ingest" >
                       |	<Source>Source_Ingest</Source>
                       |	<Target>Target_Ingest</Target>
                       |	<ComparisonKey>
                       |		<Column>name=name</Column>
                       |		<Column>rollnumber=rollnumber</Column>
                       |	</ComparisonKey>
                       |	<NumericColumns>
                       |		<Column>id=id</Column>
                       |		<Column>amount=amount</Column>
                       |	</NumericColumns>
                       |	<ReconcileColumns>
                       |		<Column>course=course</Column>
                       |	</ReconcileColumns>
                       |	<DeDuplicationStrategy side="source">
                       |		<DeDupColumns>
                       |			<Column>name</Column>
                       |			<Column>rollnumber</Column>
                       |		</DeDupColumns>
                       |		<Strategy columns="amount">max</Strategy>
                       |	</DeDuplicationStrategy>
                       |	<DeDuplicationStrategy side="target">
                       |		<DeDupColumns>
                       |			<Column>name</Column>
                       |			<Column>rollnumber</Column>
                       |		</DeDupColumns>
                       |		<Strategy columns="amount">max</Strategy>
                       |	</DeDuplicationStrategy>
                       |</Reconciliation>
                       |""" stripMargin

      val reconciliationType = scalaxb.fromXML[ReconciliationType](XML.loadString(xmlRecon))
      val reconResult = Reconciler(reconciliationType).recon(new ManagedDataset(ds1, Map("" -> "")), new ManagedDataset(ds2, Map("" -> "")))

      assert(reconResult.where(s"rowstatus=='MATCH'").count == 1)
      assert(reconResult.where(s"rowstatus=='BREAK'").count == 1)
      assert(reconResult.where(s"rowstatus=='MISSING_ON_RIGHT'").count == 2)
      assert(reconResult.where(s"rowstatus=='DUPLICATE_ON_RIGHT'").count == 1)

    }
  }


}
