package application

import common.TApplication
import controller.DataPresent

object TestExtractLanguage extends App with TApplication {
  start(){
    val dataPath = "C:/data/cdx-00000.gz"
    val contl = new DataPresent(dataPath)
    contl.dispatch()
  }
}
