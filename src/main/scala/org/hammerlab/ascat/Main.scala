package org.hammerlab.ascat

import org.apache.spark.Logging
import org.hammerlab.ascat.preprocess.AscatInput
/**
  * Created by eliza on 4/27/16.
  */
object Main extends Logging {
  private val command = AscatInput.ProcessBams

  def main(args: Array[String]): Unit = {
    command.run(args)
  }
}
