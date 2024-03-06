package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.udfs.PipelineInitCode._
import org.main.scla_dep_mgmt_change.udfs.UDFs._
import org.main.scla_dep_mgmt_change.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_19 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("first_name"),
      col("last_name"),
      col("phone"),
      col("country_code"),
      col("account_open_date"),
      col("customer_id"),
      col("account_flags"),
      col("_c3"),
      col("_c0"),
      col("_c1"),
      col("email"),
      col("_c2"),
      col("_c6"),
      col("_c4"),
      col("_c5"),
      col("_c7"),
      col("_c8"),
      col("_c9")
    )

}
