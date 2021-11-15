package common

import org.apache.spark.rdd.RDD
import util.EnvUtil

trait TDao {
  def readFile(path: String = null): RDD[String] =  {
    val (spark, sc) = EnvUtil.take()
    import spark.implicits._
    val dataRDD = if (path == null) sc.makeRDD(List("Hello Scala",
      "Hello Spark",
      "Hello Hive",
      "Hello Python",
      "Big Data PySpark",
      "Big Data Hadoop",
      "Big Data Scala")
      , 2)
    else sc.textFile(path)

    spark.sql("create database if not exists test")
    spark.sql("use test")
    dataRDD.toDF.write.mode("overwrite").saveAsTable("words")
    dataRDD
  }

}
