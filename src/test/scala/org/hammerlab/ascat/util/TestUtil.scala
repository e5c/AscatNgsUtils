package org.hammerlab.ascat.util

import java.io.File

/**
  * Created by eliza on 4/27/16.
  */
object TestUtil {
  def testDataPath(filename: String): String = {
    // If we have an absolute path, just return it.
    if (new File(filename).isAbsolute) {
      filename
    } else {
      val resource = ClassLoader.getSystemClassLoader.getResource(filename)
      if (resource == null) throw new RuntimeException("No such test data file: %s".format(filename))
      resource.getFile
    }
  }

}
