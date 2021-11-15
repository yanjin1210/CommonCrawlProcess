package common

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import util.EnvUtil

trait TApplication {
  def start(master: String = "local[*]", appName: String = "Application")(op: => Unit): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .config("spark.hadoop.parquet.enable.dictionary", "true")
      .config("spark.hadoop.parquet.enable.summary-metadata", "true")
      .config("spark.sql.parquet.filterPushdown", "true")
      .getOrCreate()

    import spark.implicits._
    import spark.sql
    val sc: SparkContext = spark.sparkContext
    EnvUtil.put(spark, sc)
//    spark.sql("create database if not exists test_local")
//    spark.sql("use test_local")

    try {op}
    catch {case ex: Throwable => {
      println(ex.getMessage)
      println(ex.getCause)
    }
    }

    spark.close()
    EnvUtil.clear()
  }
}
