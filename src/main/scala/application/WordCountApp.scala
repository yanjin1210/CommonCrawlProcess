package application

import common.TApplication
import controller.WordCountControl


object WordCountApp extends App with TApplication{
  start(){
    val wcControl = new WordCountControl()
    wcControl.dispatch()
  }
}
