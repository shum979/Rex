package transform

import common.TestSparkContextProvider
import org.apache.spark.sql.Row
import org.scalatest.{Matchers, WordSpec}
import rex.core.common.ManagedDataset
import rex.core.xml.TransformationType
import rex.src.transform.JoinTransformer

import scala.xml.XML

/**
  * Created by Shubham Gupta on 24-Feb-18.
  */
class JoinTransformTest extends WordSpec with Matchers with TestSparkContextProvider with Serializable {

  "JoinTransformTest" should {
    "Should join two dataset as per configuration" in {

      val joinTypeXml =
        """ <Transformation name="joinedData" type="JoinTransformation" applyondataref="inputDataset">
          |                    <JoinTransformation joinType="inner">
          |                        <RightSideJoiningData>JonningDataset</RightSideJoiningData>
          |                        <JoinKey leftSideKey="sellerId" rightSideKey="Seller_ID"/>
          |                        <Select>
          |                            <Column>sellerId</Column>
          |                            <Column>SellerType</Column>
          |                            <Column>SellerImpact</Column>
          |                            <Column>minMargin</Column>
          |                            <Column>maxMargin</Column>
          |                        </Select>
          |                    </JoinTransformation>
          |                </Transformation>"""

      val joinConfig = scalaxb.fromXML[TransformationType](XML.loadString(joinTypeXml))
      val sellerDataset: ManagedDataset = managedDatasetFromFile("/data/reporting_data.txt")
      val productDataset: ManagedDataset = managedDatasetFromFile("/data/product_data.txt")

      val referenceMap = Map("inputDataset" -> sellerDataset, "JonningDataset" -> productDataset)

      val result = JoinTransformer.transform(referenceMap, joinConfig)

      val expectedResult = Array(Row("9001", "Institutional", "High", "23.39", "45.13"),
        Row("9002", "inHouse", "Low", "13.34", "38.00"),
        Row("9002", "inHouse", "Low", "6.36", "17.88"),
        Row("9002", "inHouse", "Low", "9.92", "18.59"),
        Row("9003", "Premium", "Impactful", "5.62", "28.86"),
        Row("9004", "Premium", "Impactful", "22.69", "26.34"),
        Row("9005", "ChannelPartner", "VeryHigh", "24.09", "26.39"),
        Row("9005", "ChannelPartner", "VeryHigh", "23.14", "35.27"))

      result.collect() should equal(expectedResult)
    }
  }
}

