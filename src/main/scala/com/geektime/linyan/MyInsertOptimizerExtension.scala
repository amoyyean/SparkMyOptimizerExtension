package com.geektime.linyan

import org.apache.spark.sql.SparkSessionExtensions

class MyInsertOptimizerExtension  extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule(RepartitionForInsertion)
  }
}
