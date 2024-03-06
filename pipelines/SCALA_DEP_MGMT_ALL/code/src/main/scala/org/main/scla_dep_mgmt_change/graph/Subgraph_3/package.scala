package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.graph.Subgraph_3.config._
import org.main.scla_dep_mgmt_change.graph.Subgraph_3.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_3 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Script_19 = Script_19(context, in0).interim(
      "Subgraph_3",
      "QRNXqeQ6ZtVGvXZTUdN41$$7YBM1Mu7lEXhbomgUPr1d",
      "_HpiNlmJ8TGkvGuYdUCWr$$ElQ9Ra5JERAM9AyaTYKou"
    )
    val df_Script_20 = Script_20(context, df_Script_19).interim(
      "Subgraph_3",
      "4tgNLOiQHAdUAfsV7Dncf$$Cvne4IsuFj0N3FkQFMVAJ",
      "_GiRKIV3gKDezgycfL7LK$$9mm4Jw2o9BoZQR0ae8ivB"
    )
    df_Script_20
  }

}
