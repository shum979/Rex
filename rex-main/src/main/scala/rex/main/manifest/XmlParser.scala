package rex.main.manifest

import rex.core.xml.DataFlows

import scala.io.Source

/**
  * Created by Shubham Gupta on 24-Jan-18.
  * Utility class to parse XML into object model
  * this takes help of scalaXB library to parse xml.
  */
object XmlParser {

  // paring xml from file
  def parseXmlFromFile(fileName: String): DataFlows = {
    var xmlStr: String = ""
    for (line <- Source.fromFile(fileName).getLines())
      xmlStr += line

    //    checkBusinessValiditiy(xmlStr)
    parseXmlFromString(xmlStr)
  }

  def parseXmlFromString(xmlStr: String) = {
    scalaxb.fromXML[DataFlows](scala.xml.XML.loadString(xmlStr))
  }

  // parsing xml from xml string
  def parseXmlFromString(rawXmlString: String, tagMap: Map[String, String]): DataFlows = {
    var resolvedXmlStr = rawXmlString
    tagMap.foreach(entry => {
      resolvedXmlStr = resolvedXmlStr.replaceAll("\\$" + entry._1, entry._2)
    })
    parseXmlFromString(resolvedXmlStr)
  }

  def checkBusinessValiditiy(xmlStr: String) = {
    val xml = scala.xml.XML.loadString(xmlStr)
    val references = xml.\\("DataFlow").\\("@name")

    val names = references.map(_.text).toList
    val duplicates = names.groupBy(identity).collect { case (name, nameList) if nameList.lengthCompare(1) > 0 => name }

    if (!duplicates.isEmpty) {
      throw new IllegalStateException("duplicate names found in xml " + duplicates.mkString(","))
    }
  }

}
