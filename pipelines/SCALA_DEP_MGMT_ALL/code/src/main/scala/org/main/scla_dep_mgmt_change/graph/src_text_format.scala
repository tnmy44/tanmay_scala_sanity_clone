package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object src_text_format {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("text")
      .option("lineSep",                            """
""")
      .schema(StructType(Array(StructField("value", StringType, true))))
      .load("dbfs:/Prophecy/qa_data/text/textfilewithnewlineseparator")

}
