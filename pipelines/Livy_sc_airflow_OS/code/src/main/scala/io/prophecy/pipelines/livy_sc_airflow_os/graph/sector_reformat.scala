package io.prophecy.pipelines.livy_sc_airflow_os.graph

import io.prophecy.libs._
import io.prophecy.pipelines.livy_sc_airflow_os.udfs.PipelineInitCode._
import io.prophecy.pipelines.livy_sc_airflow_os.udfs.UDFs._
import io.prophecy.pipelines.livy_sc_airflow_os.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object sector_reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(lit(context.config.c1(0).c2.c3).as("Sector"))

}
