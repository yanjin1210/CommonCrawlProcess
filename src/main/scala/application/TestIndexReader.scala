package application

import common.TApplication
import dao.CDXReader


object TestIndexReader extends App with TApplication{
  start() {
    val dataPath = "C:/data/cdx-00000.gz"
    val cdxReader = new CDXReader()
    for (i <- 7 to 28 by 7 if (i != 14)) {
      val start = System.nanoTime()
      val dataDF = cdxReader.getCDXTable(dataPath, i)
      println(s"Running time for task with $i partitions is ${(System.nanoTime() - start) / 1e9d}s")
    }
  }
}
