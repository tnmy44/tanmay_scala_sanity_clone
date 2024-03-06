package com.scala.main.job12.graph

import io.prophecy.libs._
import com.scala.main.job12.udfs.PipelineInitCode._
import com.scala.main.job12.udfs.UDFs._
import com.scala.main.job12.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object CustomJoin_1 {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            col("in0.`-- c-long`") === col("in1.`-- c-long`"),
            "inner"
      )

}
