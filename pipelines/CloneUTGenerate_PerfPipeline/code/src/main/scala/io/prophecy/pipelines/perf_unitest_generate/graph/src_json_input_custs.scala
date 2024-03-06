package io.prophecy.pipelines.perf_unitest_generate.graph

import io.prophecy.libs._
import io.prophecy.pipelines.perf_unitest_generate.config.ConfigStore._
import io.prophecy.pipelines.perf_unitest_generate.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object src_json_input_custs {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("json")
      .schema(
        StructType(
          Array(
            StructField("account_flags",     StringType, true),
            StructField("account_open_date", StringType, true),
            StructField("country_code",      StringType, true),
            StructField("customer_id",       StringType, true),
            StructField("email",             StringType, true),
            StructField("first_name",        StringType, true),
            StructField("last_name",         StringType, true),
            StructField("phone",             StringType, true)
          )
        )
      )
      .load("dbfs:/Prophecy/qa_data/json/CustomersDatasetInput.json")

}
