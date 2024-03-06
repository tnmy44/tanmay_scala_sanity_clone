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

object read_separated_values {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.ProphecyDataFrame
    import org.apache.spark.sql.functions.col
    ProphecyDataFrame
      .extendedDataFrame(in)
      .readSeparatedValues(col("_c2"), List("_c7"), ":", ",")
  }

}
