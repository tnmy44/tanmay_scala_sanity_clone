package org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object src_config_catalog {

  def apply(context: Context): DataFrame = {
    val Config = context.config
    context.spark.read.table(
      s"`hive_metastore`.`${Config.CATALOG_DATABASE}`.`${Config.CATALOG_TABLENAME}`"
    )
  }

}
