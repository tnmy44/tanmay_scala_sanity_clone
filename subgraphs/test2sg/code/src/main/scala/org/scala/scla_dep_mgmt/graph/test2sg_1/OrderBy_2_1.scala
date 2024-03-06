package org.scala.scla_dep_mgmt.graph.test2sg_1

import io.prophecy.libs._
import org.main.scla_dep_mgmt.config.ConfigStore._
import org.main.scla_dep_mgmt.udfs.UDFs._
import org.main.scla_dep_mgmt.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object OrderBy_2_1 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.orderBy(
      col("`c- short`").asc,
      col("`c  - int`").asc,
      col("`- c long`").asc,
      col("`c_decimal  -  `").asc,
      col("`c_float-__  `").asc,
      col("`c -  boolean _  `").asc,
      col("c_double").asc,
      col("`c-string`").asc,
      col("`c_date-for today`").asc,
      col("`c_timestamp  __ for--today`").asc,
      col("`c_array-int  _ int`").asc,
      col("`c_array-string  _ string`").asc,
      col("`c_array--long`").asc,
      col("`c_array-- boolean `").asc,
      col("`-- c_array_timestamp -- `").asc,
      col("`c_array -- float`").asc,
      col("`c_array -- decimal`").asc,
      col("`c_struct -- _  `").asc,
      col("p_short").asc,
      col("p_int").asc,
      col("p_long").asc,
      col("p_decimal").asc,
      col("p_float").asc,
      col("p_boolean").asc,
      col("p_double").asc,
      col("p_string").asc,
      col("p_date").asc,
      col("p_timestamp").asc
    )

}
