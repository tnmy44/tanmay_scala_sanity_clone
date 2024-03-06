package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.config.Context
import org.main.scla_dep_mgmt_change.udfs.UDFs._
import org.main.scla_dep_mgmt_change.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Lookup_2 {

  def apply(context: Context, in0: DataFrame): Unit =
    createLookup("Lookup_2",
                 in0,
                 context.spark,
                 List("p_string", "p_long"),
                 "c_date-for today"
    )

}
