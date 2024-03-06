package org.main.scla_dep_mgmt.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt.graph.Subgraph_4.Subgraph_5
import org.main.scla_dep_mgmt.graph.Subgraph_4.config._
import org.main.scla_dep_mgmt.graph.Subgraph_4.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_4 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_8 = Reformat_8(context, in0).interim(
      "Subgraph_4",
      "729YXwmP8iiOGjmR9sQ8y$$_74ym9wSlPdG03pJK2UU0",
      "K9GpDkZRCJq58A4PNSywp$$r5iE3Pp5E3oGgCcSBZRLL"
    )
    val df_Subgraph_5 = Subgraph_5.apply(
      Subgraph_5.config.Context(context.spark, context.config.Subgraph_5),
      df_Reformat_8
    )
    df_Subgraph_5
  }

}
