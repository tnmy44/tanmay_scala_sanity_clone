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

object UTGenOrderBy_1 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.orderBy(
      col("c_timestamp").asc,
      expr(Config.c_orderby_expr).asc,
      lit(Config.c_orderby_int).asc,
      col("`-- c-long`").asc,
      concat(col("`c  date`"), col("c_timestamp")).asc,
      concat(col("`c  date`"), col("c_timestamp")).asc,
      col("`c   short  --`").asc,
      col("`c-int-column type`").asc,
      col("`-- c-long`").asc,
      col("`c-decimal`").asc,
      col("`c  float`").asc,
      col("`c--boolean`").asc,
      col("`c- - -double`").asc,
      col("`c___-- string`").asc,
      col("`c  date`").asc,
      col("c_timestamp").asc
    )
  }

}
