package com.scala.main.job12.graph

import io.prophecy.libs._
import com.scala.main.job12.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object src_avro {

  def apply(context: Context): DataFrame = {
    import org.apache.avro.Schema
    var reader = context.spark.read.format("avro")
    reader = reader
    reader.load("dbfs:/Prophecy/qa_data/avro/CustomersDatasetInput.avro")
  }

}
