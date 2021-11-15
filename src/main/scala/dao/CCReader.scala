package dao

import org.apache.spark.sql.DataFrame
import util.EnvUtil

class CCReader {
  val (spark, sc) = EnvUtil.take()

  def readCC(datapath:String, offsetDF: DataFrame, fileNames: Array[String]): DataFrame =
  {
    var data:DataFrame = null
    fileNames.foreach(file => {
      val dataDF = spark.read.load(datapath+file)
      data.union(dataDF)
    })
    data
  }
}
