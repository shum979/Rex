package transform

import common.TestSparkContextProvider
import org.apache.spark.sql.Row
import org.scalatest.{Matchers, WordSpec}
import rex.core.common.ManagedDataset
import rex.core.xml.TransformationType
import rex.src.transform.GroupByTransformer

import scala.xml.XML

/**
  * Created by Shubham Gupta on 24-Feb-18.
  */
class GroupByTransformTest extends WordSpec with Matchers with TestSparkContextProvider with Serializable {

  "GroupByTransformTest" should {
    "Should groupby rows as per configuration" in {

      val grpbyXml =
        """<Transformation name="grpd" type="GroupByTransformation" applyondataref="inputDataset">
          |   <GroupByTransformation>
          |      <GroupByColumn>SellerType</GroupByColumn>
          |      <AggregateColumn aggregator="COUNT" alias="Spread">sellerId</AggregateColumn>
          |   </GroupByTransformation>
          |</Transformation>"""

      val grpByConfig = scalaxb.fromXML[TransformationType](XML.loadString(grpbyXml))
      val sellerDataset: ManagedDataset = managedDatasetFromFile("/data/reporting_data.txt")

      val result = GroupByTransformer.transform(Map("inputDataset" -> sellerDataset), grpByConfig)

      val expectedResult = Array(Row("Institutional", 1),
        Row("Premium", 2),
        Row("ChannelPartner", 1),
        Row("inHouse", 1))

      result.collect() should equal(expectedResult)
    }
  }
}
