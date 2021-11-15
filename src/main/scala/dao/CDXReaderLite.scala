package dao

import org.apache.spark.sql.DataFrame
import util.EnvUtil

class CDXReaderLite {

  private val (spark, sc) = EnvUtil.take()

  import spark.implicits._

  def getCDXTable(dataPath: String): DataFrame = {
    val textRdd = sc.textFile(dataPath)
    println("Raw text:")
    textRdd.take(1).foreach(println)
    val contentRdd = textRdd.map(line => {
      val lines = line.split("[{}]")
      val time = lines(0).trim.takeRight(14)
      "{\"time\": \"" + time + "\", " + lines(1) + "}"
    })
    println("Text in json format:")
    contentRdd.take(1).foreach(println)
    val splitRdd = contentRdd.map(line => line.split(",").foreach(term => term.trim.split(":")))
    splitRdd.takeSample(false, 40).foreach(println)
    val contentDS = contentRdd.toDS
    val fieldsDF = spark.read.json(contentDS)
    fieldsDF
  }
}
