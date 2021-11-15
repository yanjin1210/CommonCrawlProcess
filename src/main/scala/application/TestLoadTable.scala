import common.TApplication
import util.EnvUtil

object TestLoadTable extends App with TApplication{
  start() {
    val (spark, sc) = EnvUtil.take()
    import spark.implicits._
    spark.sql("cache table cdx00000")
    val df = spark.sql("select languages from cdx00000")
    val languageRDD = df.rdd.flatMap(x => x.mkString.split(",").toList).map(x => (x, 1))
    languageRDD.reduceByKey(_ + _).sortBy(-_._2).saveAsTextFile("output/Languages/cdx00000")


  }
}
