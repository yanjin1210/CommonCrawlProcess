package controller

import common.TControl
import service.WordCountServ

class WordCountControl extends TControl{
  private val wcServ = new WordCountServ()
  override def dispatch(): Unit = {
    val resultArray = wcServ.dataProcessing()
    resultArray.foreach(println)
  }
}
