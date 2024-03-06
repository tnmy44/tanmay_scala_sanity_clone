package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object src_snow_DBSec {

  def apply(context: Context): DataFrame = {
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    var reader = context.spark.read
      .format("snowflake")
      .options(
        Map(
          "sfUrl" → "https://tu22760.ap-south-1.aws.snowflakecomputing.com",
          "sfUser" → dbutils.secrets
            .get(scope = "_qasecrets_snowflake", key = "username"),
          "sfPassword" → dbutils.secrets
            .get(scope = "_qasecrets_snowflake", key = "password"),
          "sfDatabase" → "QA_DATABASE",
          "sfSchema" → "QA_SCHEMA",
          "sfWarehouse" → "COMPUTE_WH",
          "sfRole" -> ""
        )
      )
    reader = reader.option("query", "select * from all_type_table limit 2")
    reader.load()
  }

}
