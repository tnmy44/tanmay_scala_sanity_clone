package com.scalaio.main.job1.graph

import io.prophecy.libs._
import com.scalaio.main.job1.config.Context
import com.scalaio.main.job1.udfs.UDFs._
import com.scalaio.main.job1.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object DONOT_DELETE {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    print("------------")
    print(Config.c_int)
    print(Config.c_string)
    print("------------")
    var out0=in0
    out0
  }

}
