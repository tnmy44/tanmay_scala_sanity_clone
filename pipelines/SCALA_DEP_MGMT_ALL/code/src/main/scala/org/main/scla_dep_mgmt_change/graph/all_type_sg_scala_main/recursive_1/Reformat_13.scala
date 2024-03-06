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

object Reformat_13 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("`c_int.test.value1`").as("value1"),
      col("`c_int.test.value1.complex-array1.diabetes`").as("diabetes"),
      col(
        "`c_int.test.value1.complex-struct1.diabetes.medication`.medicationsClasses"
      ).as("medicationsClasses")
    )

}
