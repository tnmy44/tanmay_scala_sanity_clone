package org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.udfs.PipelineInitCode._
import org.main.scla_dep_mgmt_change.udfs.UDFs._
import org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object custom_deduplicate_columns {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    in.dropDuplicates(
      List(
        "c- short",
        "c  - int",
        "- c long",
        "c_decimal  -  ",
        "c_float-__  ",
        "c -  boolean _  ",
        "c_double",
        "c-string",
        "c_date-for today",
        "c_timestamp  __ for--today",
        "c_array-int  _ int",
        "c_array-string  _ string",
        "c_array--long",
        "c_array-- boolean ",
        "-- c_array_timestamp -- ",
        "c_array -- float",
        "c_array -- decimal",
        "c_struct -- _  ",
        "p_short",
        "p_int",
        "p_long",
        "p_decimal",
        "p_float",
        "p_boolean",
        "p_double",
        "p_string",
        "p_date",
        "p_timestamp",
        "c_config_use",
        "c_config_spark_expr",
        "c_udf",
        "c- short",
        "c  - int",
        "- c long",
        "c_decimal  -  ",
        "c_float-__  ",
        "c -  boolean _  ",
        "c_double",
        "c-string",
        "c_date-for today",
        "c_timestamp  __ for--today",
        "c_array-int  _ int",
        "c_array-string  _ string",
        "c_array--long",
        "c_array-- boolean ",
        "-- c_array_timestamp -- ",
        "c_array -- float",
        "c_array -- decimal",
        "c_struct -- _  ",
        "p_short",
        "p_int",
        "p_long",
        "p_decimal",
        "p_float",
        "p_boolean",
        "p_double",
        "p_string",
        "p_date",
        "p_timestamp",
        "c_config_use",
        "c_config_spark_expr",
        "c_udf"
      )
    )
  }

}
