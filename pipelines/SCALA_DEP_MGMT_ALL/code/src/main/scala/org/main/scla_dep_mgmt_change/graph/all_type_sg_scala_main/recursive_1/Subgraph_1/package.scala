package org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main.recursive_1

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main.recursive_1.Subgraph_1.config._
import org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main.recursive_1.Subgraph_1.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_1 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_4 = Reformat_4(context, in0).interim(
      "Subgraph_1",
      "s8pD6urYcNwdhrG8AOuWm$$rNrEuygQ_6oVg62Q6S87-",
      "P9PKzpMBao4l42T4zkoef$$Sz3PY7c2ww6Pcn7CKSc76"
    )
    df_Reformat_4
  }

}
