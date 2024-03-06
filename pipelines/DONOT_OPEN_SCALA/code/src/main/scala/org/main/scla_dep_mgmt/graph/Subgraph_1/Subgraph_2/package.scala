package org.main.scla_dep_mgmt.graph.Subgraph_1

import io.prophecy.libs._
import org.main.scla_dep_mgmt.graph.Subgraph_1.Subgraph_2.config._
import org.main.scla_dep_mgmt.graph.Subgraph_1.Subgraph_2.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_2 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_5 = Reformat_5(context, in0).interim(
      "Subgraph_2",
      "jS7n56Emi4M8PQBj8HVyZ$$Zzy6ZQTizkP2ICQNUQ2cx",
      "4KMCZUeAiruTE_WVkZEiF$$qifWdX362FLGtl0EH2f34"
    )
    df_Reformat_5
  }

}
