package service

import common.TIndexFilter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

import scala.collection.mutable

class FilterJobPosts extends TIndexFilter{
  import spark.implicits._
  override def filterData(dataDF: DataFrame, dataPath: String): DataFrame = {

    val jobTerms = mutable.HashSet("job", "career", "jobs", "careers")
    val splitter = "(/|_|%|-|\\?|=|\\(|\\)|&|\\+|\\.)"
    val isJobUrl = udf((url:String) =>{
      val urlLowered = url.toLowerCase.split(splitter)
      urlLowered.exists(word => jobTerms.contains(word))
    })

    val filteredDF = dataDF.where('status === 200 && 'mime === "text/html" && isJobUrl('url))
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
