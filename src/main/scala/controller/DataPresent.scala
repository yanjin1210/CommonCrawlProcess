package controller

import common.TControl
import service.{FilterJobPosts, LangStat}

class DataPresent(val dataPath: String) extends TControl{
  override def dispatch(): Unit = {
    val data = new FilterJobPosts()
    val langData = new LangStat(data.getFiltered(dataPath))
    langData.getRes
  }
}
