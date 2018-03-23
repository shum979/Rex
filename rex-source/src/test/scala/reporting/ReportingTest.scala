package reporting

import java.io.File

import common.TestSparkContextProvider
import org.scalatest.{Matchers, WordSpec}
import rex.core.common.ManagedDataset
import rex.core.xml.ReportingType
import rex.src.reporting.Reporter

import scala.xml.XML

/**
  * Created by Shubham Gupta on 15-Feb-18.
  */
class ReportingTest extends WordSpec with Matchers with TestSparkContextProvider with Serializable {

  val projectPath = System.getProperty("user.dir")

  "ReportingTest" should {
    "Should generate basic report for given configurations" in {

      val reportingXML =
        raw"""
            <Reporting name="reportingOp" applyondataref="seller_data;product_data">
                <ReportingSql name ="NormalReport">select count(*) as spread, SellerType from seller_data group by SellerType</ReportingSql>
                <ReportingSql name ="joinedData">select ProductId,category,subcategory,procuredValue,minMargin,maxMargin,SellerImpact,lastModified from seller_data join product_data ON Seller_ID = SellerID</ReportingSql>
                <PublishTo>local</PublishTo>
                <PublishingPath>$projectPath/target</PublishingPath>
            </Reporting>
          """

      val reporting = scalaxb.fromXML[ReportingType](XML.loadString(reportingXML))

      val seller: ManagedDataset = managedDatasetFromFile("/data/reporting_data.txt")
      val product: ManagedDataset = managedDatasetFromFile("/data/product_data.txt")

      val mdMap = Map("seller_data" -> seller, "product_data" -> product)
      Reporter.report(mdMap, reporting)

      //--------------validate results------------------------
      val res: Array[File] = new File(projectPath + "/target/reportingOp").listFiles().filter(_.isDirectory)
      val latestDir = res.toSeq.sortBy(x => x.lastModified()).take(1)(0).toString
      println(latestDir)

      val output = spark.read.csv(latestDir + "/joinedData")
      output.count() shouldBe 8
    }

  }
}
