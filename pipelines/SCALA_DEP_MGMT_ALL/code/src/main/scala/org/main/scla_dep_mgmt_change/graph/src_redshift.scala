package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object src_redshift {

  def apply(context: Context): DataFrame = {
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    var reader = context.spark.read.format("jdbc")
    reader = reader
      .option("url",
              "jdbc:redshift://redshift-cluster-ashish-test.c40fu9v5qviz.us-east-1.redshift.amazonaws.com:5439/qa_database"
      )
      .option(
        "user",
        s"${dbutils.secrets.get(scope = "_qasecrets_redshift", key = "username")}"
      )
      .option(
        "password",
        s"${dbutils.secrets.get(scope = "_qasecrets_redshift", key = "password")}"
      )
      .option("pushDownPredicate", true)
      .option("driver",            "com.amazon.redshift.Driver")
    reader = reader.option("query",
                           "select * from qa_schema.all_type_only_basic limit 5"
    )
    var df = reader.load()
    df
  }

}
