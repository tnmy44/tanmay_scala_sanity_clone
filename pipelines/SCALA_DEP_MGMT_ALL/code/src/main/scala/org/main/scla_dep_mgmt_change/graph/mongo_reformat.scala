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

object mongo_reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      lit("title").as("_id"),
      lit("title").as("body"),
      lit("title").as("category"),
      lit("title").as("date"),
      lit(22).as("likes"),
      array(lit("asd"), lit("asdasd")).as("tags"),
      lit("title").as("title")
    )

}
