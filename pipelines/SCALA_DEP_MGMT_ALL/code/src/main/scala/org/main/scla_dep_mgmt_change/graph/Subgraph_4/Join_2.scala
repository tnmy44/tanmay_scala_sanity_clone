package org.main.scla_dep_mgmt_change.graph.Subgraph_4

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.udfs.PipelineInitCode._
import org.main.scla_dep_mgmt_change.udfs.UDFs._
import org.main.scla_dep_mgmt_change.graph.Subgraph_4.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Join_2 {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"), col("in0.customer_id") === col("in1._c0"), "inner")
      .select(
        col("in0.customer_id").as("customer_id"),
        col("in0.first_name").as("first_name"),
        col("in0.last_name").as("last_name"),
        col("in0.phone").as("phone"),
        col("in0.email").as("email"),
        col("in0.country_code").as("country_code"),
        col("in0.account_open_date").as("account_open_date"),
        col("in0.account_flags").as("account_flags"),
        col("in1._c0").as("_c0"),
        col("in1._c1").as("_c1"),
        col("in1._c2").as("_c2"),
        col("in1._c3").as("_c3"),
        col("in1._c4").as("_c4"),
        col("in1._c5").as("_c5"),
        col("in1._c6").as("_c6"),
        col("in1._c7").as("_c7"),
        col("in1._c8").as("_c8"),
        col("in1._c9").as("_c9")
      )

}
