package org.main.scla_dep_mgmt_change.graph.transpiler_gems

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.graph.transpiler_gems.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object cred {

  def apply(context: Context): DataFrame = {
    import _root_.io.prophecy.libs._
    generateDataFrameWithSequenceColumn(1, 100, "seq", context.spark)
  }

}
