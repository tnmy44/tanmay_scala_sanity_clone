package org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main.recursive_1.Subgraph_2_1
import org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main.recursive_1.Subgraph_1
import org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main.recursive_1.config._
import org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main.recursive_1.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object recursive_1 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_src_jdbc_mix_creds_1 = src_jdbc_mix_creds_1(context).interim(
      "recursive_1",
      "DZ_Bfb2l6024dfF6c-xTD$$ve1EEuQ99znpYquTAUPFa",
      "Lr7k56Y6FBHrctFdG_Xpu$$KxRmG6f6QJxRXg63JcOtl"
    )
    val df_very_complex_dataset = very_complex_dataset(context).interim(
      "recursive_1",
      "Zzyl2VxNguPIf8bRXuy7-$$HotJnl3EX4fo17LS0A7JI",
      "WLUuKWXWPHnuNFa04SDZ-$$qnY9HPebn3U7Mmfhdw9mT"
    )
    val df_FlattenSchema_1_2 =
      FlattenSchema_1_2(context, df_very_complex_dataset).interim(
        "recursive_1",
        "vehdNg0TFg1AOWCJmMrSY$$4lPSF9oHMVaXa--VJrUNs",
        "uBtyJRv-1w9sZ7aVV3ErM$$ad0UhLW4k6iy_Hc5Jdxj4"
      )
    val df_Reformat_13 = Reformat_13(context, df_FlattenSchema_1_2).interim(
      "recursive_1",
      "k0G6kyneWPxo7Hlkw_y6h$$76GHORvNMdRK3ovS-TqvF",
      "UZRhl2KrXOKp5m0p6DuOh$$JEYMvDRqw3bb7yY5-ju3M"
    )
    val df_Reformat_3_1 = Reformat_3_1(context, in0).interim(
      "recursive_1",
      "jiGNL3C_2hXv9zvRilmeP$$oclXGl4inUIZwm6iRcC1R",
      "k3m-wgXN7AdI1qeeM3boA$$Ztl2Qyx87BA9DWEInALli"
    )
    val df_Subgraph_2_1 = Subgraph_2_1.apply(
      Subgraph_2_1.config.Context(context.spark, context.config.Subgraph_2_1),
      df_Reformat_3_1
    )
    val df_Reformat_1_1 =
      Reformat_1_1(context, df_src_jdbc_mix_creds_1).interim(
        "recursive_1",
        "UvmRPXXCF6xxhQ6MjOkHD$$wUQeRLouYKx6Aj0MOeCO2",
        "4gEklbxuEokfVv3KE48UU$$CtYNjTtToWGlcw76RSkP0"
      )
    df_Reformat_1_1.cache().count()
    df_Reformat_1_1.unpersist()
    val df_Reformat_12 = Reformat_12(context, df_very_complex_dataset).interim(
      "recursive_1",
      "40EzUwDikmpMZBTmes6oT$$dd_w2-zQWE-e9Y12tWI2U",
      "03pqQEjtlaSIRdbDcxCJZ$$Dsow4aEceTsgbeQAjY6d-"
    )
    val df_Reformat_14 = Reformat_14(context, df_Reformat_12).interim(
      "recursive_1",
      "4Yix_IhdJtLp1F4CtgwYy$$H6VJeofsFs_s3dqOb2ALB",
      "coofe004xetvdMSU4bihs$$EREFGJuq5up-8vDh_67M0"
    )
    df_Reformat_14.cache().count()
    df_Reformat_14.unpersist()
    val df_CustomReformat_1 = CustomReformat_1(context, df_Reformat_13).interim(
      "recursive_1",
      "dOAvRQbO6m-V20ni7wRHM$$6RoMKFRMvcsf691y-BqCS",
      "h6FWr0AsHUhQ6tBblSGrl$$pm_rEPoGXMCzd2hC17BvE"
    )
    df_CustomReformat_1.cache().count()
    df_CustomReformat_1.unpersist()
    val df_Subgraph_1 = Subgraph_1.apply(
      Subgraph_1.config.Context(context.spark, context.config.Subgraph_1),
      df_Subgraph_2_1
    )
    df_Subgraph_1.cache().count()
    df_Subgraph_1.unpersist()
    df_Subgraph_2_1
  }

}
