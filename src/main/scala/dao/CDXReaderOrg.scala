/*
not working yet, require parquet file, but the input file is text file.
 */

package dao

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import util.EnvUtil

class CDXReaderOrg {
  val (spark, sc) = EnvUtil.take()
  import spark.implicits._
  import spark.sql

  val SCHEMA = StructType(Array(
    StructField("url_surtkey", StringType, true),
    StructField("url", StringType, true),
    StructField("url_host_name", StringType, true),
    StructField("url_host_tld", StringType, true),
    StructField("url_host_2nd_last_part", StringType, true),
    StructField("url_host_3rd_last_part", StringType, true),
    StructField("url_host_4th_last_part", StringType, true),
    StructField("url_host_5th_last_part", StringType, true),
    StructField("url_host_registry_suffix", StringType, true),
    StructField("url_host_registered_domain", StringType, true),
    StructField("url_host_private_suffix", StringType, true),
    StructField("url_host_private_domain", StringType, true),
    StructField("url_protocol", StringType, true),
    StructField("url_port", IntegerType, true),
    StructField("url_path", StringType, true),
    StructField("url_query", StringType, true),
    StructField("fetch_time", TimestampType, true),
    StructField("fetch_status", ShortType, true),
    StructField("fetch_redirect", StringType, true),
    StructField("content_digest", StringType, true),
    StructField("content_mime_type", StringType, true),
    StructField("content_mime_detected", StringType, true),
    StructField("content_charset", StringType, true),
    StructField("content_languages", StringType, true),
    StructField("content_truncated", StringType, true),
    StructField("warc_filename", StringType, true),
    StructField("warc_record_offset", IntegerType, true),
    StructField("warc_record_length", IntegerType, true),
    StructField("warc_segment", StringType, true),
    StructField("crawl", StringType, true),
    StructField("subset", StringType, true)
  ))

  def loadIdx(datapath: String): DataFrame = {
    val dataDF = spark.read.schema(SCHEMA).parquet("https://commoncrawl.s3.amazonaws.com/cc-index/table/cc-main/warc")
    dataDF.show(10)
    null
  }


}
