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

object join_on_title_not_c_varchar {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            col("in0.title") =!= col("in1.c_varchar"),
            "right_outer"
      )
      .select(
        col("in1.c_smallint").as("c_smallint"),
        col("in1.c_int").as("c_int"),
        col("in1.c_bigint").as("c_bigint"),
        col("in1.c_decimal").as("c_decimal"),
        col("in1.c_real").as("c_real"),
        col("in1.c_double_precision").as("c_double_precision"),
        col("in1.c_boolean").as("c_boolean"),
        col("in1.c_char").as("c_char"),
        col("in1.c_varchar").as("c_varchar"),
        col("in1.c_date").as("c_date"),
        col("in1.c_varbyte").as("c_varbyte"),
        col("in1.c_timestamp").as("c_timestamp"),
        col("in0.title").as("title")
      )

}
