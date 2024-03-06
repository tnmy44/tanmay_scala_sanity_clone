package org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main.recursive_1

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.udfs.PipelineInitCode._
import org.main.scla_dep_mgmt_change.udfs.UDFs._
import org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main.recursive_1.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_12 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("c_struct.`col-1`").as("nested.field.col1"),
      expr(
        "named_struct('a', named_struct('test1', named_struct('test- another', 2)), 'b', named_struct('test1', named_struct('test- another', 2)))"
      ).as("nested.field.col2")
    )

}
