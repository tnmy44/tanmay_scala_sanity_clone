package org.main.scla_dep_mgmt_change.graph.transpiler_gems

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.udfs.PipelineInitCode._
import org.main.scla_dep_mgmt_change.udfs.UDFs._
import org.main.scla_dep_mgmt_change.graph.transpiler_gems.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object RoundRobinPartition_1 {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) = {
    import org.apache.spark.sql.ProphecyDataFrame
    val groupedDF = ProphecyDataFrame
      .extendedDataFrame(in)
      .zipWithIndex(0, 1, "tempId", context.spark)
      .select(col("*"),
              pmod(floor(col("tempId") / lit(10)), lit(2)).as("groupId")
      )
    (groupedDF.filter(col("groupId") === lit(0)).drop("groupId").drop("tempId"),
     groupedDF.filter(col("groupId") === lit(1)).drop("groupId").drop("tempId")
    )
  }

}
