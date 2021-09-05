package com.geektime.linyan

import org.apache.spark.sql.SparkSessionExtensions

class SparkMyOptimizerExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule { session =>
      MyPushDown(session)
    }
  }
}
