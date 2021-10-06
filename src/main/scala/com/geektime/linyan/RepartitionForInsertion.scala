package com.geektime.linyan

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Repartition}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand

case class RepartitionForInsertion(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {

    plan transformDown {
      case i: InsertIntoHadoopFsRelationCommand =>
        i.withNewChildren(Seq(Repartition(numPartitions = 1, shuffle = true, child = plan)))
    }
  }
}
