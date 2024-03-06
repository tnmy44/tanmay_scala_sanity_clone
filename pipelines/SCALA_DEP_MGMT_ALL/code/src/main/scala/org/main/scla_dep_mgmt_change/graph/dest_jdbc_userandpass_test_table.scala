package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object dest_jdbc_userandpass_test_table {

  def apply(context: Context, in: DataFrame): Unit = {
    val Config = context.config
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    var writer = in.write.format("jdbc")
    writer = writer
      .option("url",      "jdbc:mysql://3.101.152.38:3306/test_database")
      .option("dbtable",  "test_table_destination2_random")
      .option("user",     s"${Config.JDBC_USER}")
      .option("password", s"${Config.JDBC_PASSWORD}")
      .option("driver",   "com.mysql.jdbc.Driver")
    writer = writer.mode("overwrite")
    writer.save()
  }

}
