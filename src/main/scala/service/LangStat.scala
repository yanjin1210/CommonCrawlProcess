package service

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import util.EnvUtil

class LangStat (val filteredData: DataFrame) {
  val (spark, sc) = EnvUtil.take()
  import spark.implicits._

  def getRes: Map[String, Int] = {
    val res: Array[Row] = filteredData.select('Language).rdd.take(1)
    println(res(0)(0))
    null
  }
}
