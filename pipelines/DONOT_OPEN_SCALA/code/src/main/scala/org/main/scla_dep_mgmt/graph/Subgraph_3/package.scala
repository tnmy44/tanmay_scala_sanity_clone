package org.main.scla_dep_mgmt.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt.graph.Subgraph_3.config._
import org.main.scla_dep_mgmt.graph.Subgraph_3.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_3 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_6 = Reformat_6(context, in0).interim(
      "Subgraph_3",
      "0NAsUT48cNkA-NaQWgh8g$$jmK0y98hmPtm87ST6aZG2",
      "y32yH0LT4LwjsKR7sxLub$$9lcS6SYU-nMRuFChZ0NvS"
    )
    df_Reformat_6
  }

}
