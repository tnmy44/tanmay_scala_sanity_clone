package org.main.scla_dep_mgmt.graph.Subgraph_4

import io.prophecy.libs._
import org.main.scla_dep_mgmt.graph.Subgraph_4.Subgraph_5.config._
import org.main.scla_dep_mgmt.graph.Subgraph_4.Subgraph_5.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_5 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_9 = Reformat_9(context, in0).interim(
      "Subgraph_5",
      "9UkCso7M03ndDIRhfqoPw$$9PVsHaKeyCA37pAMfd2gY",
      "RkKP6WsjNv_GB-jCKj5oF$$mYof6mN_pUrlFf8CLR2bH"
    )
    df_Reformat_9
  }

}
