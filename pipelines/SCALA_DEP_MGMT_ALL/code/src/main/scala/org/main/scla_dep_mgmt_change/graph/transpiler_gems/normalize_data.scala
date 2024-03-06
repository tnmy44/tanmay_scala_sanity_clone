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

object normalize_data {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.ProphecyDataFrame
    import org.apache.spark.sql.functions.lit
    ProphecyDataFrame
      .extendedDataFrame(in)
      .normalize(Some(lit(3)),
                 None,
                 None,
                 "11111",
                 List(col("seq")),
                 List().toMap,
                 List().toMap
      )
  }

}
