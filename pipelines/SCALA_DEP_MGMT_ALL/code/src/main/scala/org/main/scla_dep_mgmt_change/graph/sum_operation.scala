package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.config.Context
import org.main.scla_dep_mgmt_change.udfs.UDFs._
import org.main.scla_dep_mgmt_change.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object sum_operation {
  def apply(context: Context, in0: DataFrame, in1: DataFrame, in2: DataFrame, in3: DataFrame): (DataFrame, DataFrame, DataFrame, DataFrame) = {
    val spark = context.spark
    val Config = context.config
    var out0 = spark.createDataFrame(spark.sparkContext.makeRDD(Seq(Row(suma(1,2)))), StructType(Array(StructField("field1", IntegerType, true))))
    var out1=in1
    var out2=in2
    var out3=in3
    (out0, out1, out2, out3)
  }

}
