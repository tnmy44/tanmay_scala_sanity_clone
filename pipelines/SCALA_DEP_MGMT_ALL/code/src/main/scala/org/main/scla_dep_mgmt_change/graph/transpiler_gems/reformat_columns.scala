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

object reformat_columns {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("_c0").cast(IntegerType).as("_c0"),
              col("_c1").cast(IntegerType).as("_c1"),
              col("_c2").cast(IntegerType).as("_c2")
    )

}
