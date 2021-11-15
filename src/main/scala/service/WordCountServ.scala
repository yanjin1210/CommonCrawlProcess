package service

import common.TServ
import dao.WordCountDao
import org.apache.spark.rdd.RDD

class WordCountServ extends TServ {
  private val wcDao = new WordCountDao()

  override def dataProcessing(): Array[(String, Long)] = {

    val data: RDD[String] = wcDao.readFile()

    val mapRdd: RDD[(String, Long)] = data.flatMap(line => {line.split(" ").map((_, 1L))})

    val reduceRdd = mapRdd.reduceByKey(_+_)

    reduceRdd.collect
  }
}
