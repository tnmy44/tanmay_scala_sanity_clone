package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object src_snowflake {

  def apply(context: Context): DataFrame = {
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    var reader = context.spark.read
      .format("snowflake")
      .options(
        Map(
          "sfUrl" → "https://tu22760.ap-south-1.aws.snowflakecomputing.com",
          "sfUser" → "cicdaccount",
          "sfPassword" → "CuIqZ!9I32t@",
          "sfDatabase" → "QA_DATABASE",
          "sfSchema" → "QA_SIMPLE_SCHEMA",
          "sfWarehouse" → "COMPUTE_WH",
          "sfRole" -> "ACCOUNTADMIN"
        )
      )
    reader = reader.option("query", "select * from all_type_table_smaller")
    reader.load()
  }

}
