package org.main.scla_dep_mgmt_change.graph.pm_shared_graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.graph.pm_shared_graph.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object src_config_csv {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .schema(
        StructType(
          Array(
            StructField("customer_id",       StringType, true),
            StructField("first_name",        StringType, true),
            StructField("last_name",         StringType, true),
            StructField("phone",             StringType, true),
            StructField("email",             StringType, true),
            StructField("country_code",      StringType, true),
            StructField("account_open_date", StringType, true),
            StructField("account_flags",     StringType, true)
          )
        )
      )
      .load(context.config.CSV_DATASET_LOCATION)

}