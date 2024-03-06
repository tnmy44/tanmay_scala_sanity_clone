package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.graph.subgraph25Ports.config._
import org.main.scla_dep_mgmt_change.graph.subgraph25Ports.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object subgraph25Ports {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame,
    in3:     DataFrame,
    in4:     DataFrame,
    in5:     DataFrame,
    in6:     DataFrame,
    in7:     DataFrame,
    in8:     DataFrame,
    in9:     DataFrame,
    in10:    DataFrame,
    in11:    DataFrame,
    in12:    DataFrame,
    in13:    DataFrame,
    in14:    DataFrame,
    in15:    DataFrame,
    in16:    DataFrame,
    in17:    DataFrame,
    in18:    DataFrame,
    in19:    DataFrame,
    in20:    DataFrame,
    in21:    DataFrame,
    in22:    DataFrame,
    in23:    DataFrame,
    in24:    DataFrame
  ): (
    (
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame,
      DataFrame
    ),
    (DataFrame, DataFrame, DataFrame)
  ) = {
    val df_Reformat_1_1_1_1_7 = Reformat_1_1_1_1_7(context, in15).interim(
      "subgraph25Ports",
      "AGMEgdHeRxNgDMl3zSixC$$ako23l8lQUdpkDsTD5xmw",
      "c7jmPB9yMta9IpXjsSX0R$$5BteLZMVV34j3m2t4_VOW"
    )
    val df_Reformat_1_2 = Reformat_1_2(context, in3).interim(
      "subgraph25Ports",
      "R2YEUNtyqPmzWn6Ewstu8$$2JCGCigEZcEkIZE_Y3R1h",
      "dGXfsueoFvMEBnqXL940H$$SBWJMpGfESQPIDihz3BRl"
    )
    val df_Reformat_1_1_1_1_2 = Reformat_1_1_1_1_2(context, in24).interim(
      "subgraph25Ports",
      "PfkmbVihVaj0qhfRFM_U8$$TSsufHnCfaRxrXEcHHbbU",
      "Oap14XosIGUkfLaJoWRe6$$QD8etcd-pzCKcZY5cD7Cd"
    )
    val df_Reformat_2 = Reformat_2(context, in12).interim(
      "subgraph25Ports",
      "hfpqtJ-VKgnD43jt0PcUv$$-o_UWwq09w3CtDQ9ps_VA",
      "AhlXGiufCCZCdhqE1ZLs2$$0Z4US8EFLYBrDWF3DBeRB"
    )
    val df_Reformat_1_1_1_1_1 = Reformat_1_1_1_1_1(context, in21).interim(
      "subgraph25Ports",
      "_i_bUlZR9EeJW7h4TP89A$$erdbAL6duQ_Td2JuCmQ6Q",
      "rQZVNhadWthQK3Tclh__m$$yZ3dXWFA-2yOTq4UrZxWu"
    )
    val df_Reformat_1_1_6 = Reformat_1_1_6(context, in6).interim(
      "subgraph25Ports",
      "W31NO6DwoSJcbDjCcSo2Z$$Z3yOPOhXFe8sPPOaQeYe2",
      "Pc6acFxeASFlht1zF1g_U$$3h44n6B-1_e6PoHNwpTWJ"
    )
    val df_Reformat_1_1_1_1_4 = Reformat_1_1_1_1_4(context, in22).interim(
      "subgraph25Ports",
      "DHpvZKs-9iJVD_i_B9efa$$uzwv3QRAHu8rQbDJbYOC4",
      "IoRLtqBv7rhGKJoXBrcRn$$Nfv5eh6cW2NQST4tyyB0o"
    )
    val df_Reformat_1_1_1_1_5_1 = Reformat_1_1_1_1_5_1(context, in19).interim(
      "subgraph25Ports",
      "PtHBDluFM8doinLtxNW3_$$gKO-XW0fsxfZaip21jPgN",
      "lx1pk3vdByRjZuYFSkV4t$$CXSG6KUq0FVnueFmrIxY4"
    )
    val df_Reformat_1_1_1_1_5_2 = Reformat_1_1_1_1_5_2(context, in18).interim(
      "subgraph25Ports",
      "pAUErZscvRd3ecvpYFTSP$$tH5HtKYeCR0UhfpBdBP7H",
      "-y03ZmK8tqaMA8VlZTQZF$$v6Ry8BfLirFyo1KCcRJ2F"
    )
    val df_Reformat_1_3 = Reformat_1_3(context, in2).interim(
      "subgraph25Ports",
      "d7TJjjXcfVGHwuKeunVmz$$QoJBPrCL2Xu-hwbxTtV7T",
      "KYxkLRYlevYkECu-LJQPd$$2BVLj4o6mlkAgjxPKVCWc"
    )
    val df_Reformat_1_1_2 = Reformat_1_1_2(context, in10).interim(
      "subgraph25Ports",
      "Kl4z0BeZDElu5xXGQPpfn$$EHiOyonhz_8agvgobutNW",
      "UpHnH8z8VhsbOvqxd4gtI$$pSwlkV7ojzeedPCc_3nSa"
    )
    val df_Reformat_1_1_1_1_6 = Reformat_1_1_1_1_6(context, in14).interim(
      "subgraph25Ports",
      "OXYri9GcG-Ahqwk4RVGrP$$DTr95aA1YQbCxt4V05X3O",
      "oMlE64trLxt_hyzCVkZIL$$pKy6zsOxYYwdL8GAvHsfW"
    )
    val df_Reformat_1_1_1_1_3 = Reformat_1_1_1_1_3(context, in20).interim(
      "subgraph25Ports",
      "_yF0-f8Y8JG_MT7e7YkgY$$SPv-TSSkt7Xr9f1__Hczc",
      "-p3gim2__k-2gSfyI8Ci9$$1bD_f7XYlvmA8XLhojpb1"
    )
    val df_Reformat_1_1_1_1 = Reformat_1_1_1_1(context, in13).interim(
      "subgraph25Ports",
      "wA0so9--XMVsg4mkz9icC$$B-xheNDokSvBDZTc1D94K",
      "T1jpoQUMAelXp1JlByYpr$$fd5Xgup_EwXCSo9ZDIJrW"
    )
    val df_Reformat_1_1 = Reformat_1_1(context, in4).interim(
      "subgraph25Ports",
      "F1KB0qttomRZjPt-HReRZ$$r3l7I3vdEL6hnXUBbUAVr",
      "FC22tdVzyhd0Gu9RqeIsJ$$bUhWKEe2DNoN4KSca32J5"
    )
    val df_Reformat_1_1_7 = Reformat_1_1_7(context, in5).interim(
      "subgraph25Ports",
      "zqCNQYnHnQVvicnpadLLQ$$yd5FPO-kyoGZTdA2h2Xvn",
      "mlZ3W7cpFwN_cWHqrPgTM$$TEZ4PeWDuGpuLc8zZOQvX"
    )
    val df_Reformat_1_1_5 = Reformat_1_1_5(context, in7).interim(
      "subgraph25Ports",
      "_07RQRwb_8i_noXVM7wHN$$gqr7ws9xMHKNzBJw943SI",
      "vaOcRgc2mbtaDqf2bu-Ru$$f_DeNscx8vRZ3YRS_dSjA"
    )
    val df_Reformat_1 = Reformat_1(context, in0).interim(
      "subgraph25Ports",
      "0lUDGn10ku3QHMLfNYUgn$$3ZG5pPGGH0K3ubssk8fxe",
      "myjlh33fxkbZEDaQEW704$$OnXuN8qsF8Jxw2hn3jjzG"
    )
    val df_Reformat_1_4 = Reformat_1_4(context, in1).interim(
      "subgraph25Ports",
      "XjkM5kujHXcoGLxjoM53D$$u0PXY9BLFPwwODUS9U0GA",
      "xtduz2hcKaC2x5VdgWMu0$$-IsBbOus6NSpWEoyiA4Ty"
    )
    val df_Reformat_1_1_1 = Reformat_1_1_1(context, in11).interim(
      "subgraph25Ports",
      "MB7GKq8R7pMuVN2XL5tiX$$3P8dRKKM0UlMFA710s3b2",
      "lvaRl0DNUt_3Y-iuljmi5$$Za2tELZjFnQy5TFKBoHDw"
    )
    val df_Reformat_1_1_3 = Reformat_1_1_3(context, in9).interim(
      "subgraph25Ports",
      "csf7DdfXPCvYMjL_zeLJ8$$BGh7ll1ojYSJwZCQZhmDJ",
      "uVvtFub8WJJzkMejMWDPo$$_LtjNjxGZbyGv7LOZD3yn"
    )
    val df_Reformat_1_1_1_1_7_1 = Reformat_1_1_1_1_7_1(context, in17).interim(
      "subgraph25Ports",
      "1XRsxfoxIpDgcPjS5QRuM$$rkgx6xpJqix3k4QiXgDkr",
      "sElsplEne1JG0mieoKA75$$ONtXIk1i63FhaqxIyxCjw"
    )
    val df_Reformat_1_1_4 = Reformat_1_1_4(context, in8).interim(
      "subgraph25Ports",
      "WZa1cL93tmCPZLMbmlFJ-$$uTKwjxQzI3XE7gq_ys9-q",
      "8uJC5NQYsU8fZeHpTIttV$$sdKWpPb9tnJyEqNATOfpk"
    )
    val df_Reformat_1_1_1_1_7_2 = Reformat_1_1_1_1_7_2(context, in16).interim(
      "subgraph25Ports",
      "hcted-opDPmsS2F0iIsaq$$nfc1m-oFb4dkX-Y4W5Jz5",
      "runPfslCVPqtsLgVlUrCI$$hn7NE10R8TGbYbikzBTgM"
    )
    val df_Reformat_1_1_1_1_5 = Reformat_1_1_1_1_5(context, in23).interim(
      "subgraph25Ports",
      "WGRF8jNK5X7BJDjTbRkaN$$fS7ened5dRhu-OMgYoyWD",
      "7o_hHGQ7L2bowe9_QzQ6O$$6NJ3PNLqVxMIqKheI_Lt1"
    )
    ((df_Reformat_1,
      df_Reformat_1_4,
      df_Reformat_1_1_1_1,
      df_Reformat_1_1_1_1_3,
      df_Reformat_1_1_1_1_1,
      df_Reformat_1_1_1,
      df_Reformat_1_3,
      df_Reformat_1_2,
      df_Reformat_1_1_1_1_4,
      df_Reformat_1_1_1_1_5_2,
      df_Reformat_1_1,
      df_Reformat_1_1_3,
      df_Reformat_1_1_5,
      df_Reformat_1_1_7,
      df_Reformat_1_1_1_1_6,
      df_Reformat_1_1_2,
      df_Reformat_1_1_6,
      df_Reformat_2,
      df_Reformat_1_1_4,
      df_Reformat_1_1_1_1_2,
      df_Reformat_1_1_1_1_5,
      df_Reformat_1_1_1_1_5_1
     ),
     (df_Reformat_1_1_1_1_7_2, df_Reformat_1_1_1_1_7, df_Reformat_1_1_1_1_7_1)
    )
  }

}
