package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object src_xml {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("xml")
      .option("rowTag",           "book")
      .option("excludeAttribute", true)
      .schema(
        StructType(
          Array(
            StructField("author",       StringType, true),
            StructField("description",  StringType, true),
            StructField("genre",        StringType, true),
            StructField("price",        DoubleType, true),
            StructField("publish_date", DateType,   true),
            StructField("title",        StringType, true)
          )
        )
      )
      .load("dbfs:/Prophecy/qa_data/xml/booksdata/")

}
