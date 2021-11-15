package application

import common.TApplication
import service.{FilterJobPosts, FilterValidPosts}

object TestFilter extends App with TApplication{
  start(){
    val start = System.nanoTime()
    val dataPath = "C:/data/cdx-00000.gz"
    val filtered = new FilterValidPosts()
    val df = filtered.getFiltered(dataPath)
    df.write.mode("overwrite").text("C:/data/cc/output/")
    val time = (System.nanoTime() - start)/1e9d
    println(s"Time consumed: ${math.round(time)}s")
  }
}
