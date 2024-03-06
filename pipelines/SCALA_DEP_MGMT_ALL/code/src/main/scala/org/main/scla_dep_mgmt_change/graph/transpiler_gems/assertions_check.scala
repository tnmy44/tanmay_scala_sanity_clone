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

object assertions_check {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import _root_.io.prophecy.libs._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types.BooleanType
    var checkDF = in
    List((lit(1) === lit(2), concat(lit("sss"), lit("sss")))).zipWithIndex
      .foreach({
        case ((assertion, diagnostic), idx) =>
          checkDF = checkDF.withColumn(
            s"check_$idx",
            when(assertion.cast(BooleanType), force_error(diagnostic))
          )
      })
    checkDF.count()
    in
  }

}
