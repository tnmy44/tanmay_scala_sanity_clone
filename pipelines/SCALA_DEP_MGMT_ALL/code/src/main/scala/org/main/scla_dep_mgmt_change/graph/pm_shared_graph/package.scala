package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.graph.pm_shared_graph.Subgraph_1
import org.main.scla_dep_mgmt_change.graph.pm_shared_graph.config._
import org.main.scla_dep_mgmt_change.graph.pm_shared_graph.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object pm_shared_graph {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_src_config_csv = src_config_csv(context).interim(
      "pm_shared_graph",
      "b_tmfs0tn5u9bRng8QvCE$$oB_5dl-ujOX3PyPAOQzLT",
      "eHsLmFcs3EuVXkhPq8r_c$$2VDgkcG2RLL7KY_3EVO1E"
    )
    val df_Reformat_10 = Reformat_10(context, in0).interim(
      "pm_shared_graph",
      "Ly2IBGE1CPgoC51992Ii-$$nmbRGZ9oVlFASTMUoNGHZ",
      "dbBBb_rXY6Y3Tjj_5R8FF$$uP4-_O4RwIMeoywviDbo9"
    )
    val df_deduplicate_by_first_name =
      deduplicate_by_first_name(context, df_Reformat_10).interim(
        "pm_shared_graph",
        "8L5gE9UXhEUzs_g4yxjeX$$CY1p5j_E2XFYFioTa1UOx",
        "AyTL07DfqJajDcjV3MWxS$$RGdET24c_T8wDoC1LFBkd"
      )
    df_deduplicate_by_first_name.cache().count()
    df_deduplicate_by_first_name.unpersist()
    val df_Reformat_1 = Reformat_1(context, df_src_config_csv).interim(
      "pm_shared_graph",
      "8aE4-919ghq1tub5nQWe4$$T3bQpJFdehm223f_dTICG",
      "wB7vEfCgX1ASlEvu3VlCY$$AHE7RKSEmXTcKijStSAZX"
    )
    df_Reformat_1.cache().count()
    df_Reformat_1.unpersist()
    val df_Subgraph_1 = Subgraph_1.apply(
      Subgraph_1.config.Context(context.spark, context.config.Subgraph_1),
      df_Reformat_10
    )
    df_Subgraph_1
  }

}
