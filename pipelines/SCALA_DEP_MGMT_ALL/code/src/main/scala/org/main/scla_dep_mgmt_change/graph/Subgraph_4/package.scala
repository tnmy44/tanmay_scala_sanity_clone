package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.graph.Subgraph_4.config._
import org.main.scla_dep_mgmt_change.graph.Subgraph_4.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_4 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_ConfigVarSource = ConfigVarSource(context).interim(
      "Subgraph_4",
      "VsjzVzFVV3L0gs4WN5_YA$$PuDpDO0vhK3-enFpoVC8B",
      "YiSvVP_MrGtwJU2yC7h_l$$OOA6_mpWCsP0XvexgSnyT"
    )
    val df_Join_2 = Join_2(context, df_ConfigVarSource, in0).interim(
      "Subgraph_4",
      "5U0yZkFjW6nXX7pdasFVL$$LH_et7NmuSumXEcFHQaZp",
      "lPOy33NPqka1FH37QQwFr$$HhgvaXN2C-dp46A3s-RvW"
    )
    df_Join_2
  }

}
