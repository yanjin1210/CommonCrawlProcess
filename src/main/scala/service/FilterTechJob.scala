package service

import common.TIndexFilter
import org.apache.spark.sql.DataFrame

class FilterTechJob extends TIndexFilter{
  override def filterData(dataDF: DataFrame, dataPath: String): DataFrame = {
    dataDF
  }
}
