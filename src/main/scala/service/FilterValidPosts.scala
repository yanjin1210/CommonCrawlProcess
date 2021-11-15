package service

import common.TIndexFilter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class FilterValidPosts extends TIndexFilter{
  import spark.implicits._
  override def filterData(dataDF: DataFrame, datapath: String): DataFrame = {
    val filteredDF = dataDF.where('status === 200 && 'mime === "text/html")
      .select(
        col("time"),
        col("url"),
        col("languages"),
        col("filename"),
        col("offset"),
        col("length"))
    filteredDF
  }
}
