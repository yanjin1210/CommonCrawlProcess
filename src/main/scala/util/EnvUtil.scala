package util

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object EnvUtil {
  private val scLocal = new ThreadLocal[(SparkSession, SparkContext)]()

  def put(spark: SparkSession, sc: SparkContext): Unit = scLocal.set((spark, sc))

  def take(): (SparkSession, SparkContext) = scLocal.get()

  def clear(): Unit = scLocal.remove()
}
