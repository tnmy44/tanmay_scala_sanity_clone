package org.main.scla_dep_mgmt_change

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.config._
import org.main.scla_dep_mgmt_change.config.ConfigStore.interimOutput
import org.main.scla_dep_mgmt_change.udfs.UDFs._
import org.main.scla_dep_mgmt_change.udfs.PipelineInitCode._
import org.main.scla_dep_mgmt_change.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def graph(context: Context): Unit = {
    val df_src_parquet_all_type_and_partition_withspacehyphens_1 =
      src_parquet_all_type_and_partition_withspacehyphens_1(context).interim(
        "graph",
        "HecbJwMkIOfxVWNcUXedu$$7uG-SLLrsB0vPBhDLW1eD",
        "iH6_WLF6tZfaeyimkD5dJ$$KoLAB2hGYds83xd1gFDWc"
      )
    Lookup_2(context, df_src_parquet_all_type_and_partition_withspacehyphens_1)
    val df_src_avro_CustsDatasetInput_1_1 =
      src_avro_CustsDatasetInput_1_1(context).interim(
        "graph",
        "Pr2ZG6opSmR19tlA5wGsL$$nyW4qIFN3-K6bVP4iC0A9",
        "RHIJUX6BVWkGlL7J4XAOF$$DDUPDrhEVfS7fAFoUKQN_"
      )
    if (context.config.c_array_complex(0).car_record.carr_short > -10)
      Lookup_1(context, df_src_avro_CustsDatasetInput_1_1)
    val df_src_parquet_all_type_and_partition_withspacehyphens =
      src_parquet_all_type_and_partition_withspacehyphens(context).interim(
        "graph",
        "4c8lUyCOBMQXJt_5f7dbP$$hbMC_bA0Uq2x2q8FW4tAX",
        "pIPX9BUJ7XgmTvGgmV8Hu$$y6Sz1JeyfULGcGgi8slvs"
      )
    val df_Filter_10 = Filter_10(
      context,
      df_src_parquet_all_type_and_partition_withspacehyphens
    ).interim("graph", "V7Jv1tslSD7wNcPsY3dLt", "QLJWIrn_EA9HKXGU2Hg7d")
    val df_Reformat_4 = Reformat_4(context, df_Filter_10).interim(
      "graph",
      "QA8k3yf3NHL4Dj0WAvlcE$$6k8gyMJ_bK4ZvWD-WkPiK",
      "5857bxTW7G9sQZ8dVJJEf$$p25_pviQ-HeuDuRAg5Sv_"
    )
    df_Reformat_4.cache().count()
    df_Reformat_4.unpersist()
    val df_src_custom_csv = src_custom_csv(context).interim(
      "graph",
      "JQwp-gsQCsGchjDwIrG28$$rT5mSNWEWIE6KGpmjnXcl",
      "SiuGBTIyZKra2SA6H_FQR$$7LZ6VAXQZ3FpEPE8VNfQI"
    )
    val df_reformat = reformat(context, df_src_custom_csv).interim(
      "graph",
      "I1KkYatDxRdiN_soLDz8o$$b0KZgGPmdbkV1U7K71qEM",
      "Gxm1hZORTMBRV2G26J1os$$x3ys-NiPzPehBjp4EMMIb"
    )
    withSubgraphName("graph", context.spark) {
      withTargetId("dest_custom_csv", context.spark) {
        dest_custom_csv(context, df_reformat)
      }
    }
    val df_src_orc_all_type_no_partition = src_orc_all_type_no_partition(
      context
    ).interim("graph", "xoDRuSduB1niuIF8PP3ct", "AJqWz1Up0SmtM70ipV65i")
    val df_FlattenSchema_1 =
      FlattenSchema_1(context, df_src_orc_all_type_no_partition)
        .cache()
        .interim("graph",
                 "Szohjp1gWxHYvFe2IrcM0$$UyKRJZMAGITZk2c-3nrry",
                 "y0ljFftVNLexQytwgRtZe$$gCrSp9s-cpbqRZaLKOGRd"
        )
    val df_src_redshift = src_redshift(context).interim(
      "graph",
      "cmbNSbncQxSib9IVqGEX4$$YW0BYYpUT4c9zfwLM82io",
      "Ct_sIJHee1UFvsjTVPsPI$$huaVMhe3qBE9bIS4qNb_a"
    )
    val df_mongo_reformat = mongo_reformat(context, df_src_redshift).interim(
      "graph",
      "FwvF6GSPaeZbWKfjZ53_e$$HCDIqOWEtVZpwZe3SvvYp",
      "_s7x4w3oXzrl_7x7k7zta$$huA6FUOHNyqjmcf49I9GF"
    )
    val df_join_on_title_not_c_varchar =
      join_on_title_not_c_varchar(context, df_mongo_reformat, df_src_redshift)
        .interim("graph",
                 "FcIn1LifpacPyBhX2oqOx$$faW_Mf6-yRXhJnzdMGgwW",
                 "YhSenIOLCGgY_L5qYyNN9$$28UU22XAWduUXR6Vdl229"
        )
    val df_limit_to_12 =
      limit_to_12(context, df_join_on_title_not_c_varchar).interim(
        "graph",
        "aLUQDKrdjqiw64uym5K_V$$GILFq0I5EqeyiQyq_OyxX",
        "Byo7giF_N7zy7mzaQR5KJ$$nTJwsBHO6cmlXeT8BYSj_"
      )
    val df_src_avro_CustsDatasetInput_1 = src_avro_CustsDatasetInput_1(context)
      .interim("graph", "A7YBjCffwys4LPAleAKpC", "bsEyEzvvLYNF6CFSyeX9o")
    val df_Script_1 =
      Script_1(context, df_src_avro_CustsDatasetInput_1).interim(
        "graph",
        "zvL4eQufcf7JXDWE5naBz$$FaOjEEhgZ-ohY-GFzOtkz",
        "zvrtFJPhpuoVXq8RQ6UgB$$oj1NDu1sKgKz7Ldy31y9n"
      )
    val df_call_func = call_func(context, df_Script_1).interim(
      "graph",
      "jDGCmYurLPi5p2PI0NGES$$ppPGE4WaX6-Zibw1VKZeH",
      "kcQDaPwg6yVi98DcYU8IQ$$IoQqIwKAkSnY95_gSy2rx"
    )
    df_call_func.cache().count()
    df_call_func.unpersist()
    val df_src_snow_DBSec = src_snow_DBSec(context).interim(
      "graph",
      "fZgnTKihnC8ipfpKq-rpX$$nDzQxuoi0p31RJ7fSYDvH",
      "q1-ft0sLjJzWwbKZM32P-$$pusieZrHzORM2GGxdQVkn"
    )
    val df_WindowFunction_1 = WindowFunction_1(context, df_Filter_10).interim(
      "graph",
      "kHjXzB0HTJD1XTuwrj5kw$$ufL5LdEj0VdfG7-f1lHjQ",
      "Xh1IgHG5x-W2wGzFMUf9N$$f3xyINARrBYxOtt8dLXCx"
    )
    val df_Deduplicate_1 = Deduplicate_1(context, df_WindowFunction_1).interim(
      "graph",
      "vpuiPUloPPI5wKsdnBW2X$$9hYAzmyJqyva6xBUJQas7",
      "OcRhKi5A8GbNRmjSJ_cwb$$AAP0iEMiVDg3mvHXC8Y01"
    )
    df_Deduplicate_1.cache().count()
    df_Deduplicate_1.unpersist()
    val df_src_json_input_custs_1 = src_json_input_custs_1(context)
      .cache()
      .interim("graph", "XM4cdlXB7oVFseHwX2LRg", "gB7zngP2OXebTsbxfm4vF")
    val df_src_text_format = src_text_format(context).interim(
      "graph",
      "pFYmO62IdflFMu08UGUGP$$9oWNxkHTMUwW_X6RLmWpF",
      "qAQ6b45F1eYg2Ux205_nH$$AHCCAXcgetcMhsYPtC035"
    )
    val df_Reformat_13 = Reformat_13(context, df_src_text_format).interim(
      "graph",
      "9xeBiRLHf5e9fyulOSF9r$$gs7ERMQdJVNwPwimMkxv2",
      "10MzVgeYGoFN8queqBvoC$$7dGdHPbq8YU_CTnT6hkiE"
    )
    withSubgraphName("graph", context.spark) {
      withTargetId("dest_txt", context.spark) {
        dest_txt(context, df_Reformat_13)
      }
    }
    val df_Deduplicate_2 = Deduplicate_2(context, df_FlattenSchema_1).interim(
      "graph",
      "faSnoqDMQPRk7kfregn3H$$ZKCAqB4L-lusn5xKNicMr",
      "FI1jEKVQ6bkCmPl3ojML-$$3tJdDF9N0cONpOP-Xu-2l"
    )
    val df_SubGraph_1 = SubGraph_1.apply(
      SubGraph_1.config.Context(context.spark, context.config.SubGraph_1),
      df_Deduplicate_2
    )
    val df_src_csv_special_char_column_name =
      if (context.config.c_array_complex(0).car_record.carr_short > -10)
        src_csv_special_char_column_name(context)
          .cache()
          .interim("graph", "nEj64p7qzVS7z0LXXTFkx", "2G70-QEVG04zcV_iAsqv1")
      else null
    val df_join_and_select_columns =
      if (context.config.c_array_complex(0).car_record.carr_short > -10)
        join_and_select_columns(context,
                                df_src_csv_special_char_column_name,
                                df_src_csv_special_char_column_name
        ).interim("graph", "s6VHxJpslzbkbawETR2P-", "Vgk6IkOJ1X4GCgsQt0iEM")
      else null
    val df_SHA512 =
      if (context.config.c_array_complex(0).car_record.carr_short > -10) {
        val df_SHA1 = SHA1(context, df_join_and_select_columns).interim(
          "graph",
          "1cDWc1XK8DCPuWgeXBpjP$$hbY8yU49TC72FqKs66UNu",
          "g-d4LrpGRkD9dX_cZxA7Z$$opWXQwcEqlB_scmQJAGBW"
        )
        SHA512(context, df_SHA1).interim(
          "graph",
          "ou2yO3bowgLEGmfA0OPUz$$CC-VTtJfNxK4MXEkrtwTV",
          "ejWS6waJ30pfp5A4bmnOA$$pV-haemJ_qxkDvOTCR1sZ"
        )
      } else
        null
    val df_Script_12 = Script_12(context).interim(
      "graph",
      "RRao6fAtV9-bAAcIFHpVK$$5yft7FAPiRWuBceDEF0bo",
      "uoV-RBmBiYuGFovNAtDuJ$$0RRSxS_c9dq1385Q4pb4T"
    )
    val df_Script_3 = Script_3(context, df_Script_12).interim(
      "graph",
      "cSJj7NsQRs7_Uk5ojQzBh$$fUpC3VSO1thAtLBI1b43U",
      "NC2h6HtrgC3YA6w3xXUyN$$WDmvH8kAsS3pLUNRVkcqd"
    )
    val df_Script_7 = Script_7(context, df_Script_3).interim(
      "graph",
      "IwIKwcRg959Ki1d-igYP2$$Czi6jKwfFIDDqv8nqlhEf",
      "91MBOyLmS9gSomZqNFDrb$$S2C2OJvbbLO4HrNpJR5ub"
    )
    val df_src_xml = src_xml(context).interim(
      "graph",
      "WhLvkNUIY6uNsSdacbEVU$$KJBfjBHuqMnv28ZqOok82",
      "OQ4flw8kBa6nc7sNPiyuo$$YgOEbkT4BGPhIC-DpPSl1"
    )
    val df_src_snowflake = src_snowflake(context).interim(
      "graph",
      "52UHk_L27-2XPH7dmbKDG$$5rCEbTpHn-snIHnc3jslU",
      "hP_QZgulZQ0dARPbkRuoI$$EAkuZYLgyNPMQJCq7cpSY"
    )
    val df_Reformat_7 = Reformat_7(context, df_src_snowflake).interim(
      "graph",
      "9aa6ZtBggCK2Cjy_kaR4q$$zeHZEG4Txup06A6pPklUA",
      "DNBPYkVnbHRSEFFPI0eC7$$Da86mF3SrqXuO_XJ8f3M9"
    )
    val df_CustomGemTransformFilterCategory_1 =
      CustomGemTransformFilterCategory_1(context, df_Reformat_7).interim(
        "graph",
        "BDEJ_KZiFDF2KTtPm4ozu$$0dYY3s8yDj-Lgblg40QAp",
        "-IdkZMCELIPhu2-tslk7-$$jmsHIh9_c3WddsUpRX_ul"
      )
    val df_CustomGemRepartitionJoinSplit_1 =
      CustomGemRepartitionJoinSplit_1(context,
                                      df_CustomGemTransformFilterCategory_1
      ).interim("graph",
                "kI2b46wNPw58JlDRl-6lV$$LFKMvYQfVC57p7t3Rnxif",
                "ZzPJoSYbPzTpMCuXbQaiz$$L5J7qaG56RxjlDDKi7WL4"
      )
    val df_CustomReformat_1 =
      CustomReformat_1(context, df_CustomGemRepartitionJoinSplit_1).interim(
        "graph",
        "CcvDLcAiyY1YYZ9HC7vWk$$8JojvXzw8dUfGbrQUT6Or",
        "Bnx5K3TKp_J_kVMfj2dCl$$crOvi-SaSAby1C1BYGTgD"
      )
    val df_normalization_csv_dataset =
      normalization_csv_dataset(context).interim(
        "graph",
        "rusdZkluaY2DwOfLeH2ns$$aB11yoBpErYRN2XCfxG6r",
        "4LcDNvHWZj1ScAUtL3eqP$$cdoPUZEiZFbXD-xvQAEGb"
      )
    val df_CustomSchemaTransform_1 =
      CustomSchemaTransform_1(context, df_normalization_csv_dataset).interim(
        "graph",
        "VYlsQTtGSMwJ7495id_U8$$sr3rjrfFFE8eanrAbyjTJ",
        "mPtRDZ6fw3VPorv5ESjFn$$I9W4MO-Qzvl986Yk1a0yA"
      )
    val df_custom_join_with_hints =
      custom_join_with_hints(context,
                             df_CustomReformat_1,
                             df_CustomSchemaTransform_1,
                             df_limit_to_12
      ).interim("graph",
                "bE1cvt4h8v8GjZgiMPfK1$$OYmqbLm9XLVp3akyb3LCu",
                "vakqUWqmtmItcFPtLn9BO$$sI7nlm-gXI_0SpY_fpiUh"
      )
    val df_Repartition_2 =
      if (context.config.c_array_complex(0).car_record.carr_short > -10)
        Repartition_2(context, df_WindowFunction_1).interim(
          "graph",
          "yfeifaX7xpRlj28Ls-8Vf$$29PiihbU95u1gXWccO3GA",
          "xIA_sDLdYIaSR9jB4VXlg$$Rv5ZSnqWe9vse2SDVvmu_"
        )
      else df_WindowFunction_1
    val df_Repartition_3 = Repartition_3(context, df_Repartition_2).interim(
      "graph",
      "COz-6QYHVMxEsZK7xgFjO$$s6pqEnlwUcNFDVBX2NtnT",
      "VE-plh40cOeg9G005TysE$$b0LQzqSl6POGMQ4j4vs3c"
    )
    val df_Limit_1 =
      if (context.config.c_array_complex(0).car_record.carr_short > -10)
        Limit_1(context, df_src_csv_special_char_column_name).interim(
          "graph",
          "BtjgWEFk-IrCWsqN3RqDF",
          "vz8yOBdktTG02eFYUsCr_"
        )
      else null
    val df_Filter_1 =
      if (
        context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10 && context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10
      )
        Filter_1(context, df_src_csv_special_char_column_name).interim(
          "graph",
          "gNCO_k3OESC15dRefnTjD",
          "7ecj16KrYrMc5jCMOyV_-"
        )
      else null
    val df_UTGenOrderBy_1 =
      if (context.config.c_array_complex(0).car_record.carr_short > -10)
        UTGenOrderBy_1(context, df_src_csv_special_char_column_name).interim(
          "graph",
          "hxZRArGTe6IeA715uZ9hX",
          "NGuAUZuZAsYsYcFmYLp2B"
        )
      else null
    val df_Script_2 =
      if (
        context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10 || context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10
      )
        Script_2(context, df_Limit_1, df_Filter_1, df_UTGenOrderBy_1).interim(
          "graph",
          "K4iDT2F4Oyo-07xFirqRU$$MOT_KJUj7svHsQ2Omum0F",
          "_XsdFTaDIQf4LRvqNufg4$$4S0LETMYftOF473uP_5ql"
        )
      else null
    val df_src_parquet_all_type_no_partition =
      src_parquet_all_type_no_partition(context)
        .cache()
        .interim("graph", "PM7sxRmKo0cGk1IYdBNtT", "zMejsXna2UN-uClx0tfKO")
    val df_ComplexExpression = ComplexExpression(
      context,
      df_src_parquet_all_type_no_partition
    ).interim("graph", "2tRCXGkA-6TfjEnFofIJq", "MwSfNu4URv3g0PC7kc9CR")
    val df_src_unittest_parquet_all = src_unittest_parquet_all(context).interim(
      "graph",
      "ASVQUSGemiDjwc6M_V35W$$dFto0bYXNQ_A7n3kGM0jR",
      "U-eeoR9aJ66XXBnbWbzyj$$K894Ue9hU76yLTlb2svbu"
    )
    val df_Script_6 = Script_6(
      context,
      df_src_avro_CustsDatasetInput_1,
      df_src_avro_CustsDatasetInput_1,
      df_src_avro_CustsDatasetInput_1,
      df_src_json_input_custs_1,
      df_src_json_input_custs_1,
      df_src_json_input_custs_1
    ).interim("graph",
              "OV4umnU64FY8jeq073S6y$$_8eUIqYY7dzwH32AVBmMj",
              "tHwwGYZqVPaRNUPEYJupG$$R-CX6OC_mKEtBWa5RkLHr"
    )
    val df_Filter_6 =
      if (context.config.c_array_complex(0).car_record.carr_short > -10)
        if (context.config.c_array_complex(0).car_record.carr_short > -10)
          Filter_6(context, df_Script_6).interim(
            "graph",
            "6RHwsPMshmxqxqLVwxaAf$$qAPCAUOs1yMDoJzt6V2Ho",
            "VNyLn1sWT73jgMQlRni-X$$WRUzyI7gGT6gKhQbB7KaU"
          )
        else df_Script_6
      else null
    val df_Passthrough =
      if (context.config.c_array_complex(0).car_record.carr_short > -10)
        if (context.config.c_array_complex(0).car_record.carr_short < -10)
          Passthrough(context, df_Filter_6)
            .cache()
            .interim("graph",
                     "Xv4Z5feT9H4MCQo14Hb1L$$LAPxTWq6hZnrURpPJUW9p",
                     "wh7xjYt4N42aUnDxqkktq$$D65qeavvZrDNh7ZIvEjHb"
            )
        else df_Filter_6
      else null
    val df_Reformat_10 =
      if (context.config.c_array_complex(0).car_record.carr_short > -10) {
        val df_Filter_7 = Filter_7(context, df_Passthrough).interim(
          "graph",
          "VLWBhY76NIRf0_phA25kT$$-q0nEcXYDqZKDTTkeWb5h",
          "gSX9f0G8ErTPmmPtxVJQv$$E8sY_i15LRScWdJZ48iRI"
        )
        Reformat_10(context, df_Filter_7).interim(
          "graph",
          "Lw7klbqLQn7tohsRCkAqw$$fD1aqzic2sMGfin9HwJ1J",
          "FN8Uw0K4zC9wpRbA5Ns2h$$zAMy2i90tY89_gaKvfZn8"
        )
      } else
        null
    val df_src_snow_configs = src_snow_configs(context).interim(
      "graph",
      "uYNUuqSkSNPkoOjBP_Y7E$$jE77be9CRts_75WZgwfe_",
      "OXtiUJJJ5ey0ZCRSaiVeZ$$pBd2JmMutN19R3Cw2mydr"
    )
    val df_src_snow_UP = src_snow_UP(context).interim(
      "graph",
      "HLVATB985DGvAY5mnf4S2$$HpQ5IiXcVK_OPjuzKGTuU",
      "_N7kA-j0VQUn2yBv_zzjj$$fAwTuJdmJXYP8rbu2Rs0O"
    )
    val df_join_with_exclusion = join_with_exclusion(context,
                                                     df_src_snow_configs,
                                                     df_src_snow_DBSec,
                                                     df_src_snow_UP
    ).interim("graph",
              "sJLCrl_hbYS9bi-cXfeAm$$QRLPqr-Ri9iPbK04WIMP8",
              "bg5S-khTigKoMwoSpzGlt$$CYML4whgZrEoYLHxrby1-"
    )
    val df_Script_1_1 =
      Script_1_1(context, df_normalization_csv_dataset).interim(
        "graph",
        "bn6OrxLvCRhd7PVJgw2WA$$GTBN5S223fgBkmFFGrnoL",
        "bmYzKdbCLdTJrO8o7dQTr$$OLLs9M9xNSN30rUyisSwk"
      )
    val df_bucketed_random_projection_lsh_join =
      bucketed_random_projection_lsh_join(context, df_Script_1_1).interim(
        "graph",
        "oal3Et-6d51tvnDW0W0PT$$uDOEkQ9UplPz9HCZnGnae",
        "9EgmAq_QMV6th4l5ovSz0$$o8C5pbA10XPTdXOIah4qp"
      )
    val df_Reformat_3_1 =
      Reformat_3_1(context, df_bucketed_random_projection_lsh_join).interim(
        "graph",
        "dIMW3Rgwn0YcESSyhOwRm$$ykiwvc_x8MdZA1cy2Ltol",
        "AHIKADXkY44ID1ZMV-bDq$$rDMyuw3clKzsmHvIbsXYO"
      )
    val df_Reformat_3_1_1 = Reformat_3_1_1(context, df_Reformat_3_1).interim(
      "graph",
      "cINQjqBve1hrFYPokFJ3J$$owAHUKXRyybh9D86VBCja",
      "p3UXgBeXqOF7nFO-LaaZX$$pomqDFFTplBpX0VkcT_J_"
    )
    val df_select_udf_matrices_vectors_matrix =
      select_udf_matrices_vectors_matrix(context, df_Reformat_3_1).interim(
        "graph",
        "lW4zgi52W_qSIwZldIssn$$0ei8bW2ZTodo0Db8OF6JU",
        "83v6f3vqJzdS3Tt9vtFn9$$fQSmkl1yNcqXbZhf-4365"
      )
    val df_Script_12_1 = Script_12_1(context).interim(
      "graph",
      "nCrxuIc-Tmk2bb4CAzQ-b$$bYGtrcauW3HQdrz0rLJL8",
      "1Jpk8w_P0a_pf6wOk9qKI$$aFkzeiGkLZCdjHNwXAVbB"
    )
    val df_Script_8 = Script_8(context, df_Script_7)
      .cache()
      .interim("graph",
               "kjm6cQJLBQ7If2meAD1CY$$DPNGtLdU3Jc8CmZXCYvMh",
               "p6tB4A_Qo8pcYbrr9KwOZ$$59JxpTAUw5ISzCVua2GE5"
      )
    val (df_Script_9_out0, df_Script_9_out1, df_Script_9_out2) = {
      val (df_Script_9_out0_temp,
           df_Script_9_out1_temp,
           df_Script_9_out2_temp
      ) = Script_9(context, df_Script_12_1, df_Script_8)
      (df_Script_9_out0_temp.interim(
         "graph",
         "PvlsRiVwRhRzBwXppuR5F$$F5qjPdQS9Bhw0sTvPJYdS",
         "eIfQ2xQxKvhTrUBJ8bi3R$$_Sl7quLnKhmAjqKmXFnrS"
       ),
       df_Script_9_out1_temp.interim(
         "graph",
         "PvlsRiVwRhRzBwXppuR5F$$F5qjPdQS9Bhw0sTvPJYdS",
         "wlS8lQQWy8co_GFEynXjc$$ilwDip5ONP3QoEsoMu248"
       ),
       df_Script_9_out2_temp.interim(
         "graph",
         "PvlsRiVwRhRzBwXppuR5F$$F5qjPdQS9Bhw0sTvPJYdS",
         "Lo6MjOg15j8TkyAtHqbcN$$eOKgM8m7INFfudQQ_vlm_"
       )
      )
    }
    val df_Subgraph_3 = Subgraph_3.apply(
      Subgraph_3.config.Context(context.spark, context.config.Subgraph_3),
      df_Script_9_out0
    )
    val df_Script_16 = Script_16(context, df_Subgraph_3).interim(
      "graph",
      "9nWzQRJJA5t5wjl3ercNJ$$5yZEgZZ8xb6xoNauyiNFY",
      "hF5lafO74RNjM137_3FzV$$Lj6SLpr_G3TWQZUpeoINF"
    )
    val df_Script_18 = Script_18(context, df_Script_16).interim(
      "graph",
      "7sz7u2AKp_LSxDpFCP3BL$$OCuxF50g_AUC8Traj4RfO",
      "8qMPLVl9IdXwwk1Gp36D8$$DzqCLY2yVOPskYiNV5UtG"
    )
    val df_Script_10 =
      if (context.config.c_array_complex(0).car_record.carr_short > -10)
        if (context.config.c_array_complex(0).car_record.carr_short > -10)
          Script_10(context, df_Script_9_out1).interim(
            "graph",
            "95FDNhx9IiEJ0aROYHlwR$$EbhnAT4iXEz-NIvjDN4yV",
            "wrtyAizwYRmMFppyE1CRC$$pCYf5aATA149gvn85_Lzd"
          )
        else df_Script_9_out1
      else null
    val df_Script_13 =
      if (context.config.c_array_complex(0).car_record.carr_short > -10) {
        val df_Script_11 = Script_11(context, df_Script_10).interim(
          "graph",
          "8WRlIwKnB4PSJfaF8HPps$$IdWBJMb2YtxfW4Z6rbS6s",
          "QbEchblewqOHmci2XbdX8$$9iukH3VA35ffZsOBtaT1S"
        )
        Script_13(context, df_Script_11).interim(
          "graph",
          "hoqNQQo5U08Fy5vFOLVI6$$HeKUyd2atKXo2USe5rNkm",
          "D0f72MZW9_HT0wgpV2csR$$wxK-Q7bqih0GE4BnKrPG_"
        )
      } else
        null
    val df_Script_17 = Script_17(context, df_Script_9_out2).interim(
      "graph",
      "rk2vOthe2UTH1jROrvhxi$$rOTXHJiN102NYZhbuSHuA",
      "7dTdVfYtspgO4ia2svYSb$$jEgNfKpZfXqaQS_VXcAVk"
    )
    val df_SchemaTransform_1 =
      SchemaTransform_1(context, df_SubGraph_1).interim(
        "graph",
        "X5J7daeiMxXXaSTwpkDu9$$bZ2O5xQ9NUIlbSkMbv1MY",
        "02JU61gNQIeeb7iKJc5Z5$$_IQVWieQ7Tfs3dWmwjS8M"
      )
    val df_src_config_catalog = src_config_catalog(context).interim(
      "graph",
      "PRjDVj173nob3xUGnKjlz$$UEOoBZIEQLc38UDSHJUpB",
      "dYNRh01NEMdKNe1fYskIW$$LhxUIpu6jloDD5UGriwa5"
    )
    val df_src_config_csv = src_config_csv(context).interim(
      "graph",
      "fvQW0e5YxPn39pLJ80KXh$$GrXl4Zh5pdwNmceHeQj0w",
      "4F_WZg3NStm5aGvIcbM3j$$7YJbj8t_RY18-On4_-uvD"
    )
    val df_limit_to_two = limit_to_two(context, df_src_config_csv).interim(
      "graph",
      "xiLalB4pBY349p6MTI6hS$$ZCIfer9_ipACLdayLn8cY",
      "jd6jrE8-ak9gRAIyu_TFA$$orUtoHIrCG2MtxVf8XGdf"
    )
    val df_join_by_c_string_and_first_name =
      join_by_c_string_and_first_name(context,
                                      df_src_config_catalog,
                                      df_limit_to_two
      ).interim("graph",
                "3L-1WJAqoiy5B_UfzCb4U$$toVt75HpNGlWTLOC2xLMu",
                "3UkSqu0px3dGqe6Pwe7Vk$$ItcKXF0DwnB8Qek-3nsS7"
      )
    val (df_RowDistributor_1_out0, df_RowDistributor_1_out1) = {
      val (df_RowDistributor_1_out0_temp, df_RowDistributor_1_out1_temp) =
        RowDistributor_1(context, df_WindowFunction_1)
      (df_RowDistributor_1_out0_temp
         .cache()
         .interim("graph",
                  "mxj3GIDr8L_wSjsWZxCzw$$ly00a3LzUT_45X1UWdYHu",
                  "out0"
         ),
       df_RowDistributor_1_out1_temp
         .cache()
         .interim("graph",
                  "mxj3GIDr8L_wSjsWZxCzw$$ly00a3LzUT_45X1UWdYHu",
                  "out1"
         )
      )
    }
    val df_OrderBy_4 = OrderBy_4(context, df_RowDistributor_1_out0).interim(
      "graph",
      "hWAQiNOptWzlVEkn-huuJ$$q-JVLZQnREXaW_I25BSj5",
      "Z4XurMdUJO-ETinTrSyh6$$CxMPDJXpvul_WH8wvOY5y"
    )
    val df_Limit_3 = Limit_3(context, df_RowDistributor_1_out1).interim(
      "graph",
      "Fveo5Vzi24BOiUGy55PZU$$9-p9OnU3R-r06567Tejez",
      "Ft6cGNpKOE2Nirw_XS9_R$$Cd22cRsabCIorU61swO2N"
    )
    val df_OrderBy_3 = OrderBy_3(context, df_ComplexExpression).interim(
      "graph",
      "hx5wO_87IAH8xNU8kd6u0$$NZNcKwMNB77oH_rUMhHw2",
      "3QwZLdav6axGl_0dxx9N_$$CvmPmUKZnuBwQ1mj-VrK0"
    )
    val df_Aggregate_1 = Aggregate_1(context, df_OrderBy_3)
      .cache()
      .interim("graph",
               "n0VmJXrJcJhCDBbma0KdJ$$k94j1JSMRlVwaZ6r7RhGb",
               "Q5r1wB6YPEzdTGEVZN4dU$$Z-m7r-v8lekG4XOCQmIO2"
      )
    val (df_SQLStatement_1_out,
         df_SQLStatement_1_out1,
         df_SQLStatement_1_out2
    ) = {
      val (df_SQLStatement_1_out_temp,
           df_SQLStatement_1_out1_temp,
           df_SQLStatement_1_out2_temp
      ) =
        SQLStatement_1(context, df_Aggregate_1, df_Aggregate_1, df_Aggregate_1)
      (df_SQLStatement_1_out_temp.interim(
         "graph",
         "azjqEoM7Qc0DPek_wQxSJ$$nfc8jjh6tJ-azZxb5MxeI",
         "mJUUEuSZ4lWJ2E9rBt8sT$$nKUg8F3PmIbt1crEIYC8o"
       ),
       df_SQLStatement_1_out1_temp.interim(
         "graph",
         "azjqEoM7Qc0DPek_wQxSJ$$nfc8jjh6tJ-azZxb5MxeI",
         "bRcVgEd4wZ_Z5H6Avo0V1$$BjoiTeZgWcQIL3i5UbLP5"
       ),
       df_SQLStatement_1_out2_temp.interim(
         "graph",
         "azjqEoM7Qc0DPek_wQxSJ$$nfc8jjh6tJ-azZxb5MxeI",
         "iCDpy1CVEYIZTX27QQUGW$$mIbXM1H7DYzH9AxP3jIYL"
       )
      )
    }
    df_SQLStatement_1_out2.cache().count()
    df_SQLStatement_1_out2.unpersist()
    val df_OrderBy_6 = OrderBy_6(context, df_SQLStatement_1_out).interim(
      "graph",
      "0RMNXYh8g4Ll-VyJr8jJR$$lsQ_tpR5ggnXVOArbueFm",
      "miS1YaOwxRQTAQ3ED1fZj$$nwRJzE6C20e8cOD03DZ0y"
    )
    val df_Filter_4 = Filter_4(context, df_SQLStatement_1_out1).interim(
      "graph",
      "ciWzfAl9aodUAUcxhIsRU$$FUfG9VfSP7fv8ycEGTlrM",
      "IWrjh8haSj1CB9Wk6h9AR$$l8AFjzZT0OU-Jv2Lc_VOR"
    )
    val df_Reformat_8 = Reformat_8(context, df_Script_6).interim(
      "graph",
      "VojrEOLLM7nesHVbUffgG$$ujNbJd_Qe--3wvxRqPV3F",
      "USRNNEY4yPLG763IDrN7e$$ALfX5lz8yMMHXG3htDpVy"
    )
    val df_Filter_3 =
      if (context.config.c_array_complex(0).car_record.carr_short > -10)
        Filter_3(context, df_join_and_select_columns).interim(
          "graph",
          "ABXvUNM6audpzrMLv5LDr$$gN9X6f19SCw8Y7mEDLIgk",
          "-iLcTHeyPi06v3tqrYjBU$$7s_wmQnvqOe_tiBl8SbF_"
        )
      else null
    val (df_Subgraph_2_out0,
         df_Subgraph_2_out1,
         df_Subgraph_2_out2,
         df_Subgraph_2_out3,
         df_Subgraph_2_out4,
         df_Subgraph_2_out5,
         df_Subgraph_2_out6,
         df_Subgraph_2_out7
    ) =
      if (
        (context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10 || context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10) && context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10
      ) {
        {
          val (df_Subgraph_2_out0_temp,
               df_Subgraph_2_out1_temp,
               df_Subgraph_2_out2_temp,
               df_Subgraph_2_out3_temp,
               df_Subgraph_2_out4_temp,
               df_Subgraph_2_out5_temp,
               df_Subgraph_2_out6_temp,
               df_Subgraph_2_out7_temp
          ) = Subgraph_2.apply(
            Subgraph_2.config.Context(context.spark, context.config.Subgraph_2),
            df_Deduplicate_2,
            df_OrderBy_4,
            df_Limit_3,
            df_OrderBy_6,
            df_Filter_4,
            df_Script_2,
            df_Reformat_8,
            df_Filter_3
          )
          (df_Subgraph_2_out0_temp,
           df_Subgraph_2_out1_temp,
           df_Subgraph_2_out2_temp,
           df_Subgraph_2_out3_temp,
           df_Subgraph_2_out4_temp,
           df_Subgraph_2_out5_temp,
           df_Subgraph_2_out6_temp,
           df_Subgraph_2_out7_temp
          )
        }
      } else (null, null, null, null, null, null, null, null)
    if (df_Subgraph_2_out0 != null) {
      df_Subgraph_2_out0.cache().count()
      df_Subgraph_2_out0.unpersist()
    }
    if (df_Subgraph_2_out1 != null) {
      df_Subgraph_2_out1.cache().count()
      df_Subgraph_2_out1.unpersist()
    }
    if (df_Subgraph_2_out2 != null) {
      df_Subgraph_2_out2.cache().count()
      df_Subgraph_2_out2.unpersist()
    }
    if (df_Subgraph_2_out3 != null) {
      df_Subgraph_2_out3.cache().count()
      df_Subgraph_2_out3.unpersist()
    }
    if (df_Subgraph_2_out4 != null) {
      df_Subgraph_2_out4.cache().count()
      df_Subgraph_2_out4.unpersist()
    }
    if (df_Subgraph_2_out5 != null) {
      df_Subgraph_2_out5.cache().count()
      df_Subgraph_2_out5.unpersist()
    }
    if (df_Subgraph_2_out6 != null) {
      df_Subgraph_2_out6.cache().count()
      df_Subgraph_2_out6.unpersist()
    }
    if (df_Subgraph_2_out7 != null) {
      df_Subgraph_2_out7.cache().count()
      df_Subgraph_2_out7.unpersist()
    }
    val df_ConfigAndUDF =
      if (context.config.c_array_complex(0).car_record.carr_short > -10)
        ConfigAndUDF(context, df_Script_1).interim("graph",
                                                   "ryf6nWZatrJJgaQGDWPjC",
                                                   "bY6dBUB7OHy6i8vc1uwbD"
        )
      else df_Script_1
    val df_Reformat_6 = Reformat_6(context, df_ConfigAndUDF).interim(
      "graph",
      "_ONLavjGHI-FiiW5F1e5I$$ONx6xtQvDgAfQvU03uxoD",
      "AwdVLjh-H6KMSBB5zxS9H$$rKMotUUDG_lWkfShv395O"
    )
    val df_UTGenReformat3 =
      if (context.config.c_array_complex(0).car_record.carr_short > -10)
        UTGenReformat3(context, df_src_csv_special_char_column_name).interim(
          "graph",
          "5jhe2NMutKCmtfsWW03Dh",
          "SzVxzITqrWe5YCf4ZUe7t"
        )
      else null
    val df_src_csv_all_type_no_partition =
      src_csv_all_type_no_partition(context).interim(
        "graph",
        "da4C4FuqL-WbOpLiwTV-s$$h_fuHcKSF1lOlNmm4qn3f",
        "DKgZ2Cqz7HiV20SK0uwNR$$QYParvNcn_O6mv6rYZG4V"
      )
    val df_Subgraph_4 = Subgraph_4.apply(
      Subgraph_4.config.Context(context.spark, context.config.Subgraph_4),
      df_src_csv_all_type_no_partition
    )
    val df_Reformat_19 = Reformat_19(context, df_Subgraph_4).interim(
      "graph",
      "m_9PmDjQmWpRCTuceZeDN$$zyV0wN-w3LpBGLvvobmnH",
      "YcaOK03irRzze09dIkyRy$$0d2Y7PjXVo_N9c61m9K1f"
    )
    val df_customCatalog = customCatalog(context).interim(
      "graph",
      "kEPyXgS5a5sCumviAeDS8$$TRb8xK3ycvqrgSkqfZTb9",
      "zh-qjRfD0PW6RYfDkeFT_$$0fmBMqY6Qgtc_NzpVD5mU"
    )
    val df_CustomGemTransformFilterCategory_2 =
      CustomGemTransformFilterCategory_2(context, df_customCatalog).interim(
        "graph",
        "_3KiNEcKJOxd9IECF1C_z$$IZt20mR3bUKDaKnz2X2b7",
        "H6KkR8jCIa65p8H5NhJGM$$dsiWHnbK2jIsPiZiyd-9A"
      )
    val df_Script_21 =
      Script_21(context, df_CustomGemTransformFilterCategory_2).interim(
        "graph",
        "QoX5C2GppNODqeFLpJq_P$$ZdkDm5_ITvMw6j7fxQ7hV",
        "Y3JjiogS5-y1IcJjqXwO4$$GhlNYRX-ruVsY74L7Uwz4"
      )
    val (df_all_type_sg_scala_main_out0,
         df_all_type_sg_scala_main_out1,
         df_all_type_sg_scala_main_out2
    ) = {
      val (df_all_type_sg_scala_main_out0_temp,
           df_all_type_sg_scala_main_out1_temp,
           df_all_type_sg_scala_main_out2_temp
      ) = all_type_sg_scala_main.apply(
        all_type_sg_scala_main.config
          .Context(context.spark, context.config.all_type_sg_scala_main),
        df_Filter_10,
        df_Filter_10,
        df_Filter_10
      )
      (df_all_type_sg_scala_main_out0_temp,
       df_all_type_sg_scala_main_out1_temp,
       df_all_type_sg_scala_main_out2_temp
      )
    }
    val df_OrderBy_5 =
      OrderBy_5(context, df_all_type_sg_scala_main_out2).interim(
        "graph",
        "ob-z78F3IzAX2uhlQdAzt$$R5-sxgmSe6F4lNqqwQk1S",
        "BIK34FonH5y7wEs0h4s2W$$J9rMqqPWA4DXgpiaMTUGO"
      )
    val df_CustomDeduplicate_1 =
      CustomDeduplicate_1(context, df_custom_join_with_hints).interim(
        "graph",
        "JmEURAgPbivClUJt2vy_P$$MpeW5kaXn91Co047IkwzW",
        "ER_lrYzucTefG1B1m6Ivj$$aCyHQJ1mb8K-2DUkCO8Kk"
      )
    val df_Reformat_18 =
      if (suma(1, 2) == 3)
        Reformat_18(context, df_src_text_format).interim(
          "graph",
          "c3CVvxRiIvyWHlKaKBVpc$$69wX0iXYQPqO1324cKO55",
          "aswDBVCOMaonzRkiPfKOK$$LzUT0Ez9zsH0dk30eYzcV"
        )
      else df_src_text_format
    val df_src_fixed_format = src_fixed_format(context).interim(
      "graph",
      "UOfyV4t4Ge9nk6C5b85VM$$Q_kAXY4zmKYjFDUo_96Ry",
      "MSM2WldA3XxlXIdFQs6ya$$dUmCTkNNjn6G36E-eTKjx"
    )
    val (df_sum_operation_out0,
         df_sum_operation_out1,
         df_sum_operation_out2,
         df_sum_operation_out3
    ) = {
      val (df_sum_operation_out0_temp,
           df_sum_operation_out1_temp,
           df_sum_operation_out2_temp,
           df_sum_operation_out3_temp
      ) = sum_operation(context,
                        df_Reformat_18,
                        df_join_with_exclusion,
                        df_src_xml,
                        df_src_fixed_format
      )
      (df_sum_operation_out0_temp.interim(
         "graph",
         "EWv_aNc0s8XOvqNKUR81V$$zTCMY-Cd1bCDjY7JX7oHa",
         "_JWLYJPV7c51cE7pfm3wB$$4YldSS5t-3zJi6SQe4wFP"
       ),
       df_sum_operation_out1_temp.interim(
         "graph",
         "EWv_aNc0s8XOvqNKUR81V$$zTCMY-Cd1bCDjY7JX7oHa",
         "TXDJdjWyp5zqIW8TCYjMg$$Rrvzf2i8XuUHytHqN3vPG"
       ),
       df_sum_operation_out2_temp.interim(
         "graph",
         "EWv_aNc0s8XOvqNKUR81V$$zTCMY-Cd1bCDjY7JX7oHa",
         "vrYojeizM9_Dop-sjc2Oj$$hBcTY9dFhsknCiKp4eUsY"
       ),
       df_sum_operation_out3_temp.interim(
         "graph",
         "EWv_aNc0s8XOvqNKUR81V$$zTCMY-Cd1bCDjY7JX7oHa",
         "R5eRSozzGbEiOqk73J6C9$$F3Np5UCqwLri2DcvcTOuR"
       )
      )
    }
    df_sum_operation_out1.cache().count()
    df_sum_operation_out1.unpersist()
    df_sum_operation_out2.cache().count()
    df_sum_operation_out2.unpersist()
    df_sum_operation_out3.cache().count()
    df_sum_operation_out3.unpersist()
    val (subgraph25PortsArg0, subgraph25PortsArg1) =
      if (
        context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10 && ((context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10 || context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10) && context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10) && ((context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10 || context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10) && context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10) && context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10 && ((context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10 || context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10) && context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10) && ((context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10 || context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10) && context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10) && ((context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10 || context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10) && context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10) && context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10 && context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10 && ((context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10 || context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10) && context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10) && ((context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10 || context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10) && context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10)
      ) {
        {
          val (subgraph25PortsTempArg0, subgraph25PortsTempArg1) =
            subgraph25Ports.apply(
              subgraph25Ports.config
                .Context(context.spark, context.config.subgraph25Ports),
              df_sum_operation_out0,
              df_Reformat_3_1_1,
              df_select_udf_matrices_vectors_matrix,
              df_Script_18,
              df_Script_13,
              df_Script_17,
              df_SchemaTransform_1,
              df_join_by_c_string_and_first_name,
              df_Repartition_3,
              df_Subgraph_2_out0,
              df_Subgraph_2_out1,
              df_Reformat_6,
              df_Subgraph_2_out3,
              df_Subgraph_2_out6,
              df_Subgraph_2_out7,
              df_SHA512,
              df_UTGenReformat3,
              df_Reformat_10,
              df_Reformat_19,
              df_Subgraph_2_out4,
              df_Subgraph_2_out5,
              df_OrderBy_5,
              df_Repartition_3,
              df_CustomDeduplicate_1,
              df_Script_21
            )
          val (df_subgraph25Ports_out0_temp,
               df_subgraph25Ports_out1_temp,
               df_subgraph25Ports_out2_temp,
               df_subgraph25Ports_out3_temp,
               df_subgraph25Ports_out4_temp,
               df_subgraph25Ports_out5_temp,
               df_subgraph25Ports_out6_temp,
               df_subgraph25Ports_out7_temp,
               df_subgraph25Ports_out8_temp,
               df_subgraph25Ports_out9_temp,
               df_subgraph25Ports_out10_temp,
               df_subgraph25Ports_out11_temp,
               df_subgraph25Ports_out12_temp,
               df_subgraph25Ports_out13_temp,
               df_subgraph25Ports_out14_temp,
               df_subgraph25Ports_out15_temp,
               df_subgraph25Ports_out16_temp,
               df_subgraph25Ports_out17_temp,
               df_subgraph25Ports_out18_temp,
               df_subgraph25Ports_out19_temp,
               df_subgraph25Ports_out20_temp,
               df_subgraph25Ports_out21_temp
          ) = subgraph25PortsTempArg0
          val (df_subgraph25Ports_out22_temp,
               df_subgraph25Ports_out23_temp,
               df_subgraph25Ports_out24_temp
          ) = subgraph25PortsTempArg1
          ((df_subgraph25Ports_out0_temp,
            df_subgraph25Ports_out1_temp,
            df_subgraph25Ports_out2_temp,
            df_subgraph25Ports_out3_temp,
            df_subgraph25Ports_out4_temp,
            df_subgraph25Ports_out5_temp,
            df_subgraph25Ports_out6_temp,
            df_subgraph25Ports_out7_temp,
            df_subgraph25Ports_out8_temp,
            df_subgraph25Ports_out9_temp,
            df_subgraph25Ports_out10_temp,
            df_subgraph25Ports_out11_temp,
            df_subgraph25Ports_out12_temp,
            df_subgraph25Ports_out13_temp,
            df_subgraph25Ports_out14_temp,
            df_subgraph25Ports_out15_temp,
            df_subgraph25Ports_out16_temp,
            df_subgraph25Ports_out17_temp,
            df_subgraph25Ports_out18_temp,
            df_subgraph25Ports_out19_temp,
            df_subgraph25Ports_out20_temp,
            df_subgraph25Ports_out21_temp
           ),
           (df_subgraph25Ports_out22_temp,
            df_subgraph25Ports_out23_temp,
            df_subgraph25Ports_out24_temp
           )
          )
        }
      } else
        ((null,  null, null, null, null, null, null, null, null, null, null,
           null, null, null, null, null, null, null, null, null, null, null),
         (null,  null, null)
        )
    val (df_subgraph25Ports_out0,
         df_subgraph25Ports_out1,
         df_subgraph25Ports_out2,
         df_subgraph25Ports_out3,
         df_subgraph25Ports_out4,
         df_subgraph25Ports_out5,
         df_subgraph25Ports_out6,
         df_subgraph25Ports_out7,
         df_subgraph25Ports_out8,
         df_subgraph25Ports_out9,
         df_subgraph25Ports_out10,
         df_subgraph25Ports_out11,
         df_subgraph25Ports_out12,
         df_subgraph25Ports_out13,
         df_subgraph25Ports_out14,
         df_subgraph25Ports_out15,
         df_subgraph25Ports_out16,
         df_subgraph25Ports_out17,
         df_subgraph25Ports_out18,
         df_subgraph25Ports_out19,
         df_subgraph25Ports_out20,
         df_subgraph25Ports_out21
    ) = subgraph25PortsArg0
    val (df_subgraph25Ports_out22,
         df_subgraph25Ports_out23,
         df_subgraph25Ports_out24
    ) = subgraph25PortsArg1
    if (df_subgraph25Ports_out0 != null) {
      df_subgraph25Ports_out0.cache().count()
      df_subgraph25Ports_out0.unpersist()
    }
    if (df_subgraph25Ports_out1 != null) {
      df_subgraph25Ports_out1.cache().count()
      df_subgraph25Ports_out1.unpersist()
    }
    if (df_subgraph25Ports_out2 != null) {
      df_subgraph25Ports_out2.cache().count()
      df_subgraph25Ports_out2.unpersist()
    }
    if (df_subgraph25Ports_out3 != null) {
      df_subgraph25Ports_out3.cache().count()
      df_subgraph25Ports_out3.unpersist()
    }
    if (df_subgraph25Ports_out4 != null) {
      df_subgraph25Ports_out4.cache().count()
      df_subgraph25Ports_out4.unpersist()
    }
    if (df_subgraph25Ports_out5 != null) {
      df_subgraph25Ports_out5.cache().count()
      df_subgraph25Ports_out5.unpersist()
    }
    if (df_subgraph25Ports_out6 != null) {
      df_subgraph25Ports_out6.cache().count()
      df_subgraph25Ports_out6.unpersist()
    }
    if (df_subgraph25Ports_out7 != null) {
      df_subgraph25Ports_out7.cache().count()
      df_subgraph25Ports_out7.unpersist()
    }
    if (df_subgraph25Ports_out8 != null) {
      df_subgraph25Ports_out8.cache().count()
      df_subgraph25Ports_out8.unpersist()
    }
    if (df_subgraph25Ports_out9 != null) {
      df_subgraph25Ports_out9.cache().count()
      df_subgraph25Ports_out9.unpersist()
    }
    if (df_subgraph25Ports_out10 != null) {
      df_subgraph25Ports_out10.cache().count()
      df_subgraph25Ports_out10.unpersist()
    }
    if (df_subgraph25Ports_out11 != null) {
      df_subgraph25Ports_out11.cache().count()
      df_subgraph25Ports_out11.unpersist()
    }
    if (df_subgraph25Ports_out12 != null) {
      df_subgraph25Ports_out12.cache().count()
      df_subgraph25Ports_out12.unpersist()
    }
    if (df_subgraph25Ports_out13 != null) {
      df_subgraph25Ports_out13.cache().count()
      df_subgraph25Ports_out13.unpersist()
    }
    if (df_subgraph25Ports_out14 != null) {
      df_subgraph25Ports_out14.cache().count()
      df_subgraph25Ports_out14.unpersist()
    }
    if (df_subgraph25Ports_out15 != null) {
      df_subgraph25Ports_out15.cache().count()
      df_subgraph25Ports_out15.unpersist()
    }
    if (df_subgraph25Ports_out16 != null) {
      df_subgraph25Ports_out16.cache().count()
      df_subgraph25Ports_out16.unpersist()
    }
    if (df_subgraph25Ports_out17 != null) {
      df_subgraph25Ports_out17.cache().count()
      df_subgraph25Ports_out17.unpersist()
    }
    if (df_subgraph25Ports_out18 != null) {
      df_subgraph25Ports_out18.cache().count()
      df_subgraph25Ports_out18.unpersist()
    }
    if (df_subgraph25Ports_out19 != null) {
      df_subgraph25Ports_out19.cache().count()
      df_subgraph25Ports_out19.unpersist()
    }
    if (df_subgraph25Ports_out20 != null) {
      df_subgraph25Ports_out20.cache().count()
      df_subgraph25Ports_out20.unpersist()
    }
    if (df_subgraph25Ports_out21 != null) {
      df_subgraph25Ports_out21.cache().count()
      df_subgraph25Ports_out21.unpersist()
    }
    if (df_subgraph25Ports_out22 != null) {
      df_subgraph25Ports_out22.cache().count()
      df_subgraph25Ports_out22.unpersist()
    }
    if (df_subgraph25Ports_out23 != null) {
      df_subgraph25Ports_out23.cache().count()
      df_subgraph25Ports_out23.unpersist()
    }
    if (df_subgraph25Ports_out24 != null) {
      df_subgraph25Ports_out24.cache().count()
      df_subgraph25Ports_out24.unpersist()
    }
    val df_src_dep_csv = src_dep_csv(context).interim(
      "graph",
      "ElHeBk2t5xolEPlX3BxLl$$eREM8jyZpF_T59WhGzTxd",
      "cfpi9MV_u1vaMmB17g5UK$$kTO68YDjRHN7Pu6ZABAx3"
    )
    val df_src_catalog_table_test_catalog_source =
      src_catalog_table_test_catalog_source(context).interim(
        "graph",
        "vmRKV0Nd6-lh2PBG9DCyM$$r1IXlDV-WCE_ANbMUvQcs",
        "RiRMBmFnYhsz_jKLJYl4P$$UkVrz2ppXuwUgrdRSOXCa"
      )
    val df_Reformat_2 =
      Reformat_2(context, df_src_catalog_table_test_catalog_source).interim(
        "graph",
        "6Znk7A4h43eh2eyod9GFr$$FUM5E0P9Xrn_WDFN9qFVz",
        "r0a543LnVEGLj4EiHP7P0$$F6WweoNFKHeQEZHINdKwA"
      )
    val df_pm_shared_graph =
      if (context.config.c_array_complex(0).car_record.carr_short < -10)
        pm_shared_graph.apply(
          pm_shared_graph.config
            .Context(context.spark, context.config.pm_shared_graph),
          df_Reformat_2
        )
      else df_Reformat_2
    df_pm_shared_graph.cache().count()
    df_pm_shared_graph.unpersist()
    val df_Reformat_11 =
      Reformat_11(context, df_src_json_input_custs_1).interim(
        "graph",
        "ZMgwtnns-tTCc6_AkDB1T$$HlJJiaf4rq7eKwaylhy2z",
        "7n1LEo5i48jo2hYsynoEj$$hvLuh8DbZi04XCXMrCU33"
      )
    val df_Removal =
      if (context.config.c_array_complex(0).car_record.carr_short < -10)
        Removal(context, df_Reformat_11).interim(
          "graph",
          "vtrt894h2cP4HPPq0tJYm$$nVyis4Nyw9hFaHcE3Uh4N",
          "HGpEJQXaGmnbqQx0-N2Ou$$4GW_iHvRg0qy6A638oKv3"
        )
      else null
    val df_Script_5 = Script_5(context, df_all_type_sg_scala_main_out0).interim(
      "graph",
      "-1sF51qSXmJKE9XJXPJ8W$$_c2ggE1nZMrpb8USN1LhB",
      "rmzP7Y-i4QV7gVAmbc_Zj$$C30YN2Ip6C7uzw0o94s3c"
    )
    df_Script_5.cache().count()
    df_Script_5.unpersist()
    val df_Reformat_12_1 =
      if (context.config.c_array_complex(0).car_record.carr_short < -10) {
        val df_Filter_8 = Filter_8(context, df_Removal).interim(
          "graph",
          "7lZxGiUTo9kqXwLJTm6om$$16ViJbMn5im4un4UtmlRW",
          "4L5R--Ky50NdDTNsyFyyF$$qcXXjNMaNmzaHpkrsio9-"
        )
        Reformat_12_1(context, df_Filter_8).interim(
          "graph",
          "eJUhwK4RnXbz73c-JCBxP$$WQN5Qn_p6Zqud15JFr_4j",
          "-qB_J_bjQwgQ-XrNXHXHE$$byaM8mQr4zjrTuoj1VUF7"
        )
      } else
        null
    val df_OrderBy_2 =
      if (context.config.c_array_complex(0).car_record.carr_short > -10) {
        val df_Filter_2 = Filter_2(context, df_ConfigAndUDF).interim(
          "graph",
          "F_cxTDso7G28ruB0xni7N",
          "V_c7nCHezVT96rpb-8TzQ"
        )
        OrderBy_2(context, df_Filter_2).interim("graph",
                                                "0BEbuoCU7vasdz7Wr3Ft1",
                                                "1DyiAt45y3SZCoDDCNVmw"
        )
      } else
        null
    if (df_OrderBy_2 != null) {
      df_OrderBy_2.cache().count()
      df_OrderBy_2.unpersist()
    }
    val df_Filter_9 =
      if (context.config.c_array_complex(0).car_record.carr_short < -10) {
        val df_Removesource = Removesource(context)
          .cache()
          .interim("graph",
                   "a3rUD-9k_xwUdWjK6qGou$$cQeC0zfS7_DC8Z0VxaFit",
                   "6gYqX-7AzI7laJWMoiFfc$$jMrGm_QAejmmz8HcvFzo7"
          )
        Filter_9(context, df_Removesource).interim(
          "graph",
          "YUFxqXlEb8W0N9tr5a6Na$$9LAtvfvYvsn0X8-RWkBcQ",
          "FpB16Sy1seLe_3UtHCOsq$$PIT9A3M6bwKkeqCAyfAHx"
        )
      } else
        null
    val df_SetOperation_2 =
      SetOperation_2(context, df_Script_6, df_Filter_6).interim(
        "graph",
        "0D6KA6M69NaxwLh4YHL36$$DBCL7Ij7iKu5Ru7REo8Ff",
        "bal6pUnNminCP0rLQ_Bd9$$r3-2j5Z8_uku45wIZ-gY9"
      )
    val df_deduplicate_by_first_name =
      deduplicate_by_first_name(context, df_src_dep_csv).interim(
        "graph",
        "C2jQ56pVqWl85ovoqDI0z$$1rC4xIWdZ7fEAa4g5p2H6",
        "szS9NYPzOEXKM8bNPxdoF$$645favgFJEosHdp4h2i1D"
      )
    df_deduplicate_by_first_name.cache().count()
    df_deduplicate_by_first_name.unpersist()
    val df_UTGenRepartition_1 =
      if (
        context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10 && context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10
      )
        if (context.config.c_array_complex(0).car_record.carr_short > -10)
          UTGenRepartition_1(context, df_src_csv_special_char_column_name)
            .interim("graph",         "JDdGnXnYzeiw8aXJ5gB5q", "sNPPOS1ix-ZWOvC-Q0cPD")
        else df_src_csv_special_char_column_name
      else null
    if (df_UTGenRepartition_1 != null) {
      df_UTGenRepartition_1.cache().count()
      df_UTGenRepartition_1.unpersist()
    }
    val df_src_delta_all_type_no_partition = src_delta_all_type_no_partition(
      context
    ).interim("graph", "Fmg6g-ViOm77hFxIpAPch", "ee0X8XILMHhyMro4U1_V7")
    val df_RemoveSG = RemoveSG.apply(
      RemoveSG.config.Context(context.spark, context.config.RemoveSG),
      df_Reformat_11
    )
    val df_Reformat_15 = Reformat_15(context, df_Reformat_11).interim(
      "graph",
      "3AlcfhRj5qf_uGewn4qxB$$duHHgOw88DaWl1IHl8BkD",
      "haPxfb0CBtaNcQo_OHmHT$$XJ3HoO58qBqlOUAbmZcra"
    )
    df_Reformat_15.cache().count()
    df_Reformat_15.unpersist()
    val df_SetOperation_2_1 =
      SetOperation_2_1(context, df_SetOperation_2, df_SetOperation_2).interim(
        "graph",
        "oBvXdYTA5tBj61w6P7m-9$$PW6Ikm92ofDdAGVfMvL_m",
        "TE4HOZUCVuefZj5jvGqDR$$AT2IGIqwYHf-R6lTUfBkJ"
      )
    df_SetOperation_2_1.cache().count()
    df_SetOperation_2_1.unpersist()
    val df_Reformat_5 = Reformat_5(context, df_Aggregate_1).interim(
      "graph",
      "ZohJ-uI1fzL3XQKSP_Umt$$4GCMZbeofo2GO4U1MPXs6",
      "F5wuTXsiXBnYYKYOJ4870$$M0xPxSk9xE2BT2wb6Va5r"
    )
    val df_Filter_5 = Filter_5(context, df_Reformat_5).interim(
      "graph",
      "IeVyvyMj40jBGOIg_RQiH$$JKL7hZbfQtkLCtw62jh3v",
      "u4GgmkkIhWUyZ8fAu8j6s$$hK0j23KHvKHzqt9nB9PWy"
    )
    df_Filter_5.cache().count()
    df_Filter_5.unpersist()
    val df_expressions_sg = expressions_sg.apply(
      expressions_sg.config
        .Context(context.spark, context.config.expressions_sg),
      df_src_json_input_custs_1
    )
    df_expressions_sg.cache().count()
    df_expressions_sg.unpersist()
    val df_src_jdbc_dbsecrets_test_table =
      src_jdbc_dbsecrets_test_table(context).interim(
        "graph",
        "NG_B4o8Q0Iv0D4o5YYpCr$$n87rkkbtCOdOILtRAM8vk",
        "L51AITeK59ABBzSIypaTR$$m7-FKe757702YEY0py7hR"
      )
    if (context.config.c_array_complex(0).car_record.carr_short > -10) {
      withSubgraphName("graph", context.spark) {
        withTargetId("dest_jdbc_userandpass_test_table", context.spark) {
          dest_jdbc_userandpass_test_table(context,
                                           df_src_jdbc_dbsecrets_test_table
          )
        }
      }
    }
    if (df_Filter_9 != null) {
      df_Filter_9.cache().count()
      df_Filter_9.unpersist()
    }
    val df_SetOperation_1 = SetOperation_1(context,
                                           df_src_delta_all_type_no_partition,
                                           df_src_delta_all_type_no_partition
    ).interim("graph",
              "6MGeoO_3CAkDyZvPYAGOY$$W7ZInyQSxiNrP-XuCd9hK",
              "9nSC-MM8SEHYGTWCQwmW8$$82qHa1CcCeW1MGcYp5jb4"
    )
    Script_4(context, df_SetOperation_1)
    val df_UTGenAllType =
      UTGenAllType(context, df_src_unittest_parquet_all).interim(
        "graph",
        "pKHRYS1hsbrGwDD8pGo75$$3oG41nwykeKi4E0wRs4tA",
        "0uOZZEovz1kGAC3K7yB_7$$SZHrPVYA60sLjrwDRHuoE"
      )
    df_UTGenAllType.cache().count()
    df_UTGenAllType.unpersist()
    val df_R_Filter_11 =
      if (context.config.c_array_complex(0).car_record.carr_short < -10)
        R_Filter_11(context, df_RemoveSG).interim(
          "graph",
          "uoRgh4baOsleaWphQ9OIX$$yKstMzrxjVMnwtwC6b6h8",
          "Aur067G0rGfYaw_qRn7X1$$xfGFyO5PoYdKAo7uA7BQg"
        )
      else null
    if (df_R_Filter_11 != null) {
      df_R_Filter_11.cache().count()
      df_R_Filter_11.unpersist()
    }
    val df_Reformat_12 =
      if (context.config.c_array_complex(0).car_record.carr_short < -10)
        Reformat_12(context, df_Removal).interim(
          "graph",
          "WZYz0FxCdqDp1yTyM8sJN$$I4la6Fg8ZWiIkF6YedSf_",
          "ioP2s_WBg03JF1j96As0j$$bkbL6sVWEQ7oaPNVBUcwP"
        )
      else null
    if (df_Reformat_12 != null) {
      df_Reformat_12.cache().count()
      df_Reformat_12.unpersist()
    }
    if (df_Reformat_12_1 != null) {
      df_Reformat_12_1.cache().count()
      df_Reformat_12_1.unpersist()
    }
    val df_Reformat_1 =
      Reformat_1(context, df_all_type_sg_scala_main_out1).interim(
        "graph",
        "suL54pumFBm-vMC_7rPqj$$LnB0spZUTAA5qGaKWizGA",
        "2M0DrPW0rnsiTSzHVchx9$$a5mOBh06lRQYCfJ9cx0mW"
      )
    df_Reformat_1.cache().count()
    df_Reformat_1.unpersist()
    transpiler_gems.apply(
      transpiler_gems.config
        .Context(context.spark, context.config.transpiler_gems)
    )
    val df_src_xlsx_main = src_xlsx_main(context).interim(
      "graph",
      "-VRpC_HNCj-_RHaiofvlF$$90WMZD_eHf3WcL9-SLPtw",
      "qS8LAsamDhO4nFlO1MYXW$$7Fw3KQ2YIeWUFnu946Kxx"
    )
    withSubgraphName("graph", context.spark) {
      withTargetId("dest_xlsx_target", context.spark) {
        dest_xlsx_target(context, df_src_xlsx_main)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
      .newSession()
    val context = Context(spark, config)
    MetricsCollector.initializeMetrics(spark)
    implicit val interimOutputConsole: InterimOutput = InterimOutputHive2("")
    spark.conf.set("prophecy.collect.basic.stats",          "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules",
                   "org.apache.spark.sql.catalyst.optimizer.ColumnPruning"
    )
    spark.conf.set("spark_config1",
                   "spark./<>;'\"[]{}\\|~*/-+p- config1 value !~_#@%^&*()-="
    )
    spark.conf.set("spark_config2",     "spark_config2_value")
    spark.conf.set("fs.s3a.access.key", "AKIAR6ESAR2JAQNZNVMH")
    spark.conf
      .set("fs.s3a.secret.key", "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")
    spark.conf
      .set("prophecy.metadata.pipeline.uri", "pipelines/SCALA_DEP_MGMT_ALL")
    spark.sparkContext.hadoopConfiguration.set(
      "hadoop_config1",
      "hadoop./<>;'\"[]{}\\|~*/-+p- config1 value !~_#@%^&*()-="
    )
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config2", "hadoop_config2_value")
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3a.access.key", "AKIAR6ESAR2JAQNZNVMH")
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3a.secret.key", "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")
    try MetricsCollector.start(spark,
                               "pipelines/SCALA_DEP_MGMT_ALL",
                               context.config
    )
    catch {
      case _: Throwable =>
        MetricsCollector.start(spark, "pipelines/SCALA_DEP_MGMT_ALL")
    }
    graph(context)
    MetricsCollector.end(spark)
  }

}
