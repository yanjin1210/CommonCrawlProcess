package dao

import org.apache.spark.sql.DataFrame
import util.{EnvUtil, SearchTerms}

class CDXReader {

  private val (spark, sc) = EnvUtil.take()

  import spark.implicits._
  val FIELDS: Array[String] = Array("time", "url", "languages", "status","mime", "filename", "offset", "length")

  def getCDXTable(dataPath: String, numOfPartitions: Int = 8): DataFrame = {
    val textRdd = sc.textFile(dataPath).repartition(numOfPartitions)
    println("Raw text:")
    textRdd.take(1).foreach(println)
    val contentRdd = textRdd.map(line => {
      val lines = line.split("[{]")
      val time = lines(0).trim.takeRight(14)
      "{\"time\": \"" + time + "\", " + lines(1)
    })
    println("Text in json format:")

    val contentDS = contentRdd.toDS
    val fieldsDF = spark.read.json(contentDS).select('time, 'url, 'status, 'languages, 'mime, 'filename, 'offset, 'length)
    fieldsDF.take(10).foreach(println)
    fieldsDF
  }
}