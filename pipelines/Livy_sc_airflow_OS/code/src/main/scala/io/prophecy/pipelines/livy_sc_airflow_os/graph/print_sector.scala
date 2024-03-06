package io.prophecy.pipelines.livy_sc_airflow_os.graph

import io.prophecy.libs._
import io.prophecy.pipelines.livy_sc_airflow_os.config.Context
import io.prophecy.pipelines.livy_sc_airflow_os.udfs.UDFs._
import io.prophecy.pipelines.livy_sc_airflow_os.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object print_sector {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    print(col("Sector"))
    val out0=in0
    out0
  }

}
