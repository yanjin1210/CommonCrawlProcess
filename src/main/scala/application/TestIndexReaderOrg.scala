package application

import common.TApplication
import dao.{CDXReader, CDXReaderOrg}


object TestIndexReaderOrg extends App with TApplication{
  start(){
    val dataPath = "C:/data/cdx-00000.gz"
    val cdxReader = new CDXReaderOrg()
    val dataDF = cdxReader.loadIdx(dataPath)
  }
}
