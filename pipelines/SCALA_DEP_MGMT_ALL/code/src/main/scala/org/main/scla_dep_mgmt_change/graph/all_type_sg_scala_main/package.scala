package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main.recursive_1
import org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main.config._
import org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object all_type_sg_scala_main {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame
  ): Subgraph3 = {
    val df_src_csv_all_type_no_partition_1_1 =
      src_csv_all_type_no_partition_1_1(context).interim(
        "all_type_sg_scala_main",
        "JLY1tO1i21EZKEVS01Lxr$$U2y4-lQJbIazEiYx6-7Ok",
        "zvBq0LKYIcDK8AdbA4m1e$$4v2Gs41WdOellrnTh1bLy"
      )
    Lookup_1_1(context, df_src_csv_all_type_no_partition_1_1)
    val df_src_csv_all_type_no_partition_1 =
      src_csv_all_type_no_partition_1(context).interim(
        "all_type_sg_scala_main",
        "A9rElKDgLrUiv9NV-Gb_N$$RhLoa7fWLsy8ZrKSG_ETH",
        "0NwLDePbq8XoET6rLo-n3$$XntpDSKpb7YGNBuvfs2US"
      )
    val df_Reformat_1_1 =
      Reformat_1_1(context, df_src_csv_all_type_no_partition_1).interim(
        "all_type_sg_scala_main",
        "WwV8YcgqfDbTr1Qjw-_ub$$Frf-6ptRWeW5lktM6o6iJ",
        "y-_P0LZWdJ6sodhEZaaQ6$$D4yP-beggRQDICXgM2NHG"
      )
    val df_Reformat_2_1 = Reformat_2_1(context, in0).interim(
      "all_type_sg_scala_main",
      "hUP1i867OpQpUfEFUJCQy$$PsiH5rqxis-77MJiDjEAJ",
      "IOGnwjIuO1YjIb-sLJyha$$L0tnnwlkLeUmivtpnWxZe"
    )
    val df_Filter_1_1 = Filter_1_1(context, df_Reformat_2_1).interim(
      "all_type_sg_scala_main",
      "lgmqnTtMR3qcVjlH2Ep35$$BJygf5Sdg9caBJmEYMi5f",
      "UULbpwesGwistLf0QlHPk$$r09pBZoZaTB074IT0c9rP"
    )
    val df_OrderBy_1_1 = OrderBy_1_1(context, df_Filter_1_1).interim(
      "all_type_sg_scala_main",
      "l-CYWkwAxWLBq7BtZXNmc$$zd1-1GjIqvz8vhg59GhQU",
      "Ddaz4jJQzSi44fZRicVSW$$r5UVUyppCEYPplGJybjPv"
    )
    val df_Limit_1_1 = Limit_1_1(context, df_OrderBy_1_1).interim(
      "all_type_sg_scala_main",
      "4zrXJ1pBVISuC3q-XDTmZ$$uFoFi_-zrV7vvBWKCKa2G",
      "g6spxFdnq3qgPqw265z3O$$u36cF1sNAKWsO0UaECvhb"
    )
    val df_WindowFunction_1_1 =
      WindowFunction_1_1(context, df_Limit_1_1).interim(
        "all_type_sg_scala_main",
        "thGcHosI_1BgWDJrHjFoV$$Z8jdf9MPJKWvamOhGLqZk",
        "gtWt2VSgvUB0J7hcxUL0n$$DuwPaYJO36Z2c0-DRv2nC"
      )
    val df_SetOperation_1_1 = SetOperation_1_1(context,
                                               df_WindowFunction_1_1,
                                               df_WindowFunction_1_1
    ).interim("all_type_sg_scala_main",
              "2364u3XyEfqkcOFGPdruQ$$D3Tt41b9B7MgOmE5r7cjg",
              "Vd1zEA7MfS5nw7LTlqhDD$$nwjMMYLvMYggia3AMuoT2"
    )
    val df_SQLStatement_1 =
      SQLStatement_1(context, df_SetOperation_1_1).interim(
        "all_type_sg_scala_main",
        "LcKgbTCXM4s71Zw5K7_U-$$obdQ9okthMdvYwn2otban",
        "SMB10nqnNy5840kTondLS$$joFRTxwV1sntopLfLNC45"
      )
    val df_CustomGemRepartitionJoinSplit_1 =
      CustomGemRepartitionJoinSplit_1(context, df_SQLStatement_1).interim(
        "all_type_sg_scala_main",
        "QQroUjwwD1--uzVvuWcph$$Qt-X40JOizqIUPIHU3Ue9",
        "gDd_baeGF_JrbaZsToBCg$$SxvybkY0Sxq6gUJmpacL7"
      )
    df_CustomGemRepartitionJoinSplit_1.cache().count()
    df_CustomGemRepartitionJoinSplit_1.unpersist()
    val df_PassMeBuddy =
      if (context.config.c_sg1_c_int > -100)
        PassMeBuddy(context, df_Filter_1_1).interim(
          "all_type_sg_scala_main",
          "hlCiNUUN4-N1dhJqgjw_N$$mja6O7vN0kDjxCa9BrN2v",
          "WIIEsa_6EAsbIWCfQEA1i$$b2hm1FyGLjMyegY1rRyAq"
        )
      else df_Filter_1_1
    val df_DontPassMeBuddy = DontPassMeBuddy(context, df_PassMeBuddy).interim(
      "all_type_sg_scala_main",
      "WZs6WELaNojtI-wCXD4ua$$1v5Gl3mtPqOScupSqhwtc",
      "SLbPjIl69GfVbK7OVkka2$$4coi27cSeJBA_5zpeYIIC"
    )
    val df_CustomReformat_1 =
      CustomReformat_1(context, df_DontPassMeBuddy).interim(
        "all_type_sg_scala_main",
        "7vVYx_mllbRTV-JKnce9a$$dA2Ibtyx-tTaJYxKEGkIO",
        "8Qhv3JiDWZbK3x4VsLwO_$$zjoj4b6Ei_MjNv-McV-s2"
      )
    val df_Deduplicate_1_1 = Deduplicate_1_1(context, in1).interim(
      "all_type_sg_scala_main",
      "6ITI0NEHkk-C0PzFrl3JB$$MFmWHd70r4eQrkw91aca_",
      "djg3J1fGmwlsK4U4kIhZ1$$2D6ZFNjn3zQtQFXT2bxdj"
    )
    val df_Script_1_1 = Script_1_1(context, df_Deduplicate_1_1).interim(
      "all_type_sg_scala_main",
      "NhC1SRdJyLJ_imHbm-SGU$$bSJOmuzYeSoBUgkmI4I_q",
      "cix9b7hiz5GT6SHK1a2ey$$lXupXQxmwialVyfJLfaYo"
    )
    val df_Reformat_13 = Reformat_13(context, df_Script_1_1).interim(
      "all_type_sg_scala_main",
      "zMwb3QWCiADwag7iTQogC$$xuSLQC94egBIOvWHQw_pX",
      "7SEQpAGRp6L_vg09Qq1ZS$$IEsbB5CA0YP-7c-5P5ecq"
    )
    val df_Limit_211 = Limit_211(context, df_Reformat_13).interim(
      "all_type_sg_scala_main",
      "Sj7LV4uYXlw7TEyGxxooG$$PE5mbqmayJln60rPuqGGx",
      "vODwzTm3inkZrgoHkNyR_$$OGxCvll4V9ToTPT8A8lBl"
    )
    df_Limit_211.cache().count()
    df_Limit_211.unpersist()
    val df_CustomJoin_1 =
      CustomJoin_1(context, df_CustomReformat_1, df_CustomReformat_1).interim(
        "all_type_sg_scala_main",
        "5vMMMOU6sgfccC7tC91KK$$w22G9lMlrNLBnhEqgxdBl",
        "TqVhevCKg_b6xp9LMwBPG$$pthcCKn2xD3DhEjxQ8uE7"
      )
    val df_custom_deduplicate_columns =
      custom_deduplicate_columns(context, df_CustomJoin_1).interim(
        "all_type_sg_scala_main",
        "iwcQI6OsIhk7OYXBqNTEg$$7p52A7AFP7vHhzQ1yC60Q",
        "k56eLOqmi5kI-zcUaMFUY$$rfGzzn3XAvYphmm4GgnJB"
      )
    df_custom_deduplicate_columns.cache().count()
    df_custom_deduplicate_columns.unpersist()
    val df_SchemaTransform_1_1 =
      SchemaTransform_1_1(context, df_SetOperation_1_1).interim(
        "all_type_sg_scala_main",
        "Cpp-eSDWFOydUile7Uyio$$bZX7DFwfKAhkjyrQYl3hK",
        "xLJkxvbQqP8DuRgGsUUiP$$2wNBwk6hwOuVrBvyRhwRj"
      )
    val df_Join_1_1 =
      Join_1_1(context, df_SchemaTransform_1_1, df_SchemaTransform_1_1).interim(
        "all_type_sg_scala_main",
        "YsPCyfeLBsN_-gEeTFtDN$$-V7Z1TGKKNjIAqLv5qJgh",
        "RD_fpo-eYIkVlGHMOhi40$$VFoRPP8HYZ9LTkyEiIHz-"
      )
    val (df_RowDistributor_1_1_out0, df_RowDistributor_1_1_out1) = {
      val (df_RowDistributor_1_1_out0_temp, df_RowDistributor_1_1_out1_temp) =
        RowDistributor_1_1(context, df_Join_1_1)
      (df_RowDistributor_1_1_out0_temp.interim(
         "all_type_sg_scala_main",
         "PgUYSxgP6yAYJcJ3ppHfX$$qebUpUIZBuNpOJ7nry7BY",
         "HxaQX0nF88aUd633ycAPm$$z29FHBHDODVPADRySSzYu"
       ),
       df_RowDistributor_1_1_out1_temp.interim(
         "all_type_sg_scala_main",
         "PgUYSxgP6yAYJcJ3ppHfX$$qebUpUIZBuNpOJ7nry7BY",
         "PwWx57iOUAbAqGLDKsfGQ$$CIwMZ-PobnCYf4KAbzo6Z"
       )
      )
    }
    val df_src_dep_csv = src_dep_csv(context).interim(
      "all_type_sg_scala_main",
      "M9OYfKISC_FDHtb7ACJWr$$k0gR2bkr5Amgz_aZ-8BA1",
      "NMFZkoMEAgJxPO6Evb9vF$$waRsjE0gL6Tou7OLyN--l"
    )
    withSubgraphName("all_type_sg_scala_main", context.spark) {
      withTargetId("scala_random_target_subgraph_donotuse", context.spark) {
        scala_random_target_subgraph_donotuse(context, df_Reformat_1_1)
      }
    }
    val df_Reformat_1 = Reformat_1(context, df_src_dep_csv).interim(
      "all_type_sg_scala_main",
      "Fs2Tvgypjb2ami2GTLNnk$$MFBWi7Lwr4Rg4xtx-YAet",
      "HgIVT-pSLsk5xMoW0UGVR$$4624QiY9mTJLAkLcc5rv0"
    )
    df_Reformat_1.cache().count()
    df_Reformat_1.unpersist()
    val df_deduplicate_by_first_name =
      deduplicate_by_first_name(context, df_src_dep_csv).interim(
        "all_type_sg_scala_main",
        "Ok3yXJmqwzb2CfLjPjYcx$$DAAI0xSWKGEANV3y_7zSr",
        "75owA6nbcUGFIX0dNrf4N$$qLX6D2NVCszZtslQMW6hG"
      )
    df_deduplicate_by_first_name.cache().count()
    df_deduplicate_by_first_name.unpersist()
    val df_src_config_csv = src_config_csv(context).interim(
      "all_type_sg_scala_main",
      "gwOk2r6-wpGuNNajJTD2q$$P9989G-7KiSGkDQ4rgnMG",
      "nktVXf4iSuj6Us8dFpndm$$oDC4j4AbH8icHY0OF8EIV"
    )
    df_src_config_csv.cache().count()
    df_src_config_csv.unpersist()
    val df_recursive_1 = recursive_1.apply(
      recursive_1.config.Context(context.spark, context.config.recursive_1),
      df_Script_1_1
    )
    val df_Aggregate_1_1 =
      Aggregate_1_1(context, df_RowDistributor_1_1_out0).interim(
        "all_type_sg_scala_main",
        "40x82-YyIIUk7pIhht2LX$$UJNxfFPBkv96Y2m1S95Q9",
        "JSwGY5d6RoYVcwEbme7Hq$$KTC0Hm3s5NinVIkD8A6Mv"
      )
    val df_FlattenSchema_1_1 =
      FlattenSchema_1_1(context, df_Aggregate_1_1).interim(
        "all_type_sg_scala_main",
        "7dSYHS4ccxtaDQIA58yGB$$p3K8kYqJqHGRGHldWS0J_",
        "1TNe7-gys_ySzJscz90bs$$iVXdNcZkBOxfMG2SzH6zy"
      )
    val df_OrderBy_2_1 =
      OrderBy_2_1(context, df_RowDistributor_1_1_out1).interim(
        "all_type_sg_scala_main",
        "Q732qOBLhcrT2x0MvHU_E$$HjaZQLEhV5t_hEkBl9f9d",
        "Nljh_h-Q4z_oo_ltnGgSe$$Es2PKiYPyFbrCSYhybdaG"
      )
    val df_Reformat_7 = Reformat_7(context, in2).interim(
      "all_type_sg_scala_main",
      "rTy83DSuASMaEv6ATwCpc$$GcqS-aOkVsY-4eGz1-2F8",
      "5LiZljahU3bU_Rym0A-1p$$5q3z5RaXADuyvhYtshRmo"
    )
    df_Reformat_7.cache().count()
    df_Reformat_7.unpersist()
    val df_src_config_catalog = src_config_catalog(context).interim(
      "all_type_sg_scala_main",
      "dx4kYGi2a2JrjUTBdmD48$$AkZDO74ak1G5tNMKd6BEF",
      "IZkQXrJw81Mcv7ke266lR$$N111TLynA-_HMoXcElZwg"
    )
    df_src_config_catalog.cache().count()
    df_src_config_catalog.unpersist()
    (df_FlattenSchema_1_1, df_OrderBy_2_1, df_recursive_1)
  }

}
