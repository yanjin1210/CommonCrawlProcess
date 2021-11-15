package util

import scala.collection.mutable

object SearchTerms extends Serializable {
  def Search(line: String, fields: Array[String]): List[String] = {
    val termPairs = line.split(",")
    val termMap = new mutable.HashMap[String, String]()
    termPairs.foreach(term => {
      val pairs = term.split(":")
      termMap.update(pairs(0).trim.stripPrefix("\"").stripSuffix("\""), pairs(1).trim.stripPrefix("\"").stripSuffix("\""))
    })
    val valueArray = new Array[String](fields.length)
    for (i<-fields.indices) {
      valueArray(i) = termMap.getOrElse(fields(i), "")
    }
    valueArray.toList
  }

}
