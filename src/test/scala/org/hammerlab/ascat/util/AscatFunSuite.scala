package org.hammerlab.ascat.util

import org.bdgenomics.utils.misc.SparkFunSuite
import org.scalatest.Matchers

/**
  * Created by eliza on 4/27/16.
  */
trait AscatFunSuite extends SparkFunSuite with Matchers {
  override val appName: String = "ascat preprocessing"
  override val properties: Map[String, String] =
    Map(
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryo.registrator" -> "org.hammerlab.guacamole.kryo.GuacamoleKryoRegistrator",
      "spark.kryoserializer.buffer" -> "4",
      "spark.kryo.referenceTracking" -> "true"
    )
}
