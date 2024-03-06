package org.main.scla_dep_mgmt_change.graph.pm_shared_graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.graph.pm_shared_graph.Subgraph_1.config._
import org.main.scla_dep_mgmt_change.graph.pm_shared_graph.Subgraph_1.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_1 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_11 = Reformat_11(context, in0).interim(
      "Subgraph_1",
      "scquc15pv5AvPVXgGyWJy$$bPu_-3MPKOOt04zjMwhNI",
      "QMcIIElLXTnydERZY6sjL$$MsOLRe-rYA64MeTafafLM"
    )
    val df_src_jdbc_mix_creds_1 = src_jdbc_mix_creds_1(context).interim(
      "Subgraph_1",
      "cnrKWOKwwC66GPLulpCtd$$_zGQArPpMndPnuvE1OtSS",
      "RvlDTPa7u3AoLkfpKXuND$$eiebTDTgQIdzaGt5ryudQ"
    )
    val df_Reformat_1_1 =
      Reformat_1_1(context, df_src_jdbc_mix_creds_1).interim(
        "Subgraph_1",
        "OH-JtFodOZE1E3mDeXyH3$$vpFsSyTvFUtMGBq6kmPYY",
        "Yk6FoGmfY7yVEjniYgmgQ$$_AuhluXA-BZCJ0v__KEmf"
      )
    df_Reformat_1_1.cache().count()
    df_Reformat_1_1.unpersist()
    df_Reformat_11
  }

}
