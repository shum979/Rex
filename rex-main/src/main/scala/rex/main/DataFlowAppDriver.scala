package rex.main

import java.util.Calendar

import rex.main.manifest.DataFlowExecutor

/**
  * Created by Shubham Gupta on 07-Feb-18.
  */
object DataFlowAppDriver {

  def main(args: Array[String]): Unit = {
    val fileName = args(0)

    DataFlowExecutor.execute(fileName)

    println("Application completed at " + Calendar.getInstance().getTime)
  }

}
