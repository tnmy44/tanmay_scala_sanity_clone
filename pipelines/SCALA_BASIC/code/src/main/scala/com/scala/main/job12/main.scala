package com.scala.main.job12

import io.prophecy.libs._
import com.scala.main.job12.config._
import com.scala.main.job12.config.ConfigStore.interimOutput
import com.scala.main.job12.udfs.UDFs._
import com.scala.main.job12.udfs.PipelineInitCode._
import com.scala.main.job12.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def graph(context: Context): Unit = {
    if (context.config.c_int > -100) {
      val df_src_avro = src_avro(context).interim(
        "graph",
        "2CsHylb0Si7Iu1BwtU25H$$3c8Yrcik6DteZZzAs7Ogp",
        "40EaQ49eizOI6Q0sgDpV2$$tNarnJReknpOLuhqQoArm"
      )
      Lookup_1(context, df_src_avro)
    }
    val df_src_csv_special_char = src_csv_special_char(context).interim(
      "graph",
      "o8K-lNobc6Z8Asi3dRegs$$Buw8lxPhFtSUcFZhGxXbx",
      "Yui765QKx0wOKHaOnjtzk$$Q04y-nxgyv0ZfORhocEUn"
    )
    val df_schema_transform =
      schema_transform(context, df_src_csv_special_char).interim(
        "graph",
        "5IEpMUJQMpUIx6Hv3eZVS$$9f4baBrU_1q1LbFl9fY2n",
        "vx64sYjC4vVrmBOSkCOXa$$qpmxU6WcJBG1bBJ5VrLY-"
      )
    val df_CustomGemTransformFilterCategory_1 =
      CustomGemTransformFilterCategory_1(context, df_src_csv_special_char)
        .interim("graph",
                 "n_CKTPd6A9R9rPqkgt0mw$$_3M7OWRrlOSRWskZIQAs9",
                 "whcnTGwCA3VhKMsCdp6b2$$swdLZcebirgJaU4FZKqNp"
        )
    val df_reformatted_columns_1 = reformatted_columns_1.apply(
      reformatted_columns_1.config
        .Context(context.spark, context.config.reformatted_columns_1),
      df_src_csv_special_char
    )
    val df_reformat_columns =
      reformat_columns(context, df_reformatted_columns_1).interim(
        "graph",
        "9SxukrkbLjB9767nnjjyc$$HKrQPtYnAcfBjk1YLp12B",
        "q2OHFdKSlaApfiB-p2kod$$gjcVI8oVJu6r8jvarb7gm"
      )
    val df_Reformat_11 = Reformat_11(context, df_reformat_columns).interim(
      "graph",
      "Xd3Wt7qKtiH4SPH5A-eGR$$oZwwQphp8b9ewzTGxxg9_",
      "BrNFqH0kpdDq56WRlmzeU$$4SWO5koqKXjR7QpYwUKZ5"
    )
    df_Reformat_11.cache().count()
    df_Reformat_11.unpersist()
    val df_apply_function =
      if (context.config.c_int > -100)
        if (context.config.c_int > -100)
          apply_function(context, df_src_csv_special_char)
            .cache()
            .interim("graph",
                     "yyqmKt6lIbvCdCmjxPmht$$g2IqhVcYuJWg9juCxyjBj",
                     "KOUvK4u2zMac-9BLVt8Is$$CspK8C6VwzsmBNgylSibS"
            )
        else df_src_csv_special_char
      else null
    val df_reformatted_columns = if (context.config.c_int > -100) {
      val df_join_by_c_short =
        join_by_c_short(context, df_apply_function, df_schema_transform)
          .cache()
          .interim("graph",
                   "zUezgrxzF588txPNf9wMI$$JVWCUQIsrKou8fBiESaqS",
                   "hF6jZ7HJrWnao2RvRyYbB$$xQSJSKZgOIG6LRBb7CP6M"
          )
      reformatted_columns(context, df_join_by_c_short).interim(
        "graph",
        "Jsldsl3d5xD4SRjpdKI-Z$$DuCm45gqMbljkiKqBMQjw",
        "sPVo9omm0xwOufqyXCGI8$$F12AVA_FrP1_w296nytQb"
      )
    } else
      null
    val df_DONOT_DELETE =
      DONOT_DELETE(context, df_src_csv_special_char).interim(
        "graph",
        "QVkuAsxoq5NQ0MUxMEpMp$$-059MNIlT6AakSkqptOSR",
        "GDf7WQpZhqZc_HPKUwIc1$$t_F8ZP7gTEFITFhhzi1N1"
      )
    val df_CustomGemRepartitionJoinSplit_1 =
      CustomGemRepartitionJoinSplit_1(context, df_DONOT_DELETE).interim(
        "graph",
        "sbd_fYp32psHBPth3kqZy$$mPG_vGpr2r_MktDv8LRWw",
        "x2snk2pgLPg2mm2hKzMBv$$LnqXgvfKSOaYFwouGs-8k"
      )
    val df_CustomGemTransformFilterCategory_2 =
      CustomGemTransformFilterCategory_2(context,
                                         df_CustomGemTransformFilterCategory_1
      ).interim("graph",
                "EjjMHnPvrGzYls6XPFySB$$azEYjSAn39DTtRE9WW0nf",
                "MbwKOXO9-5-a5rBeo2-ZO$$bcyw9ykFGowELp74C_pf1"
      )
    df_CustomGemTransformFilterCategory_2.cache().count()
    df_CustomGemTransformFilterCategory_2.unpersist()
    val df_Subgraph_1 = Subgraph_1.apply(
      Subgraph_1.config.Context(context.spark, context.config.Subgraph_1),
      df_src_csv_special_char
    )
    val df_Subgraph_3 = Subgraph_3.apply(
      Subgraph_3.config.Context(context.spark, context.config.Subgraph_3),
      df_Subgraph_1
    )
    val df_Reformat_7 = Reformat_7(context, df_Subgraph_3).interim(
      "graph",
      "wuThqUc2qpf_FmJCMTj2e$$vhP6lu8CS3r_W42eJUyZ1",
      "H6Z_aAnftBpk4a-USAecW$$DfJIjJOqY8XnYd8OGH-ZP"
    )
    val df_Subgraph_4 = Subgraph_4
      .apply(
        Subgraph_4.config.Context(context.spark, context.config.Subgraph_4),
        df_Reformat_7
      )
      .cache()
    val df_Reformat_10 = Reformat_10(context, df_Subgraph_4).interim(
      "graph",
      "cxC3W007DVzCvlwaUSQWg$$ckcxE7qTOTY5b8BHciWQ2",
      "VJzZuiwZgT0rxBlvzkHAY$$80GGHEDnLkXPhO1zGDxko"
    )
    df_Reformat_10.cache().count()
    df_Reformat_10.unpersist()
    val df_CustomReformat_2 =
      CustomReformat_2(context, df_CustomGemRepartitionJoinSplit_1).interim(
        "graph",
        "djKFQZRlVCere97TsxtKw$$_tEwWtajgw-TitdqpB5XN",
        "4PvRQFnLj_-rouLCAb71J$$R1wzVDa8PJ1kby33I99iA"
      )
    val df_CustomReformat_1 =
      if (context.config.c_int > -100)
        CustomReformat_1(context, df_reformatted_columns).interim(
          "graph",
          "NG_KvBlzEK9mR_1FUbHNQ$$UozfSELim_YbY2dONwhe8",
          "ZQx1JGu7AcT4x8RSxLL_L$$gEvcvCz2lniJjLeq_ck03"
        )
      else null
    if (df_CustomReformat_1 != null) {
      df_CustomReformat_1.cache().count()
      df_CustomReformat_1.unpersist()
    }
    val df_CustomSchemaTransform_1 =
      if (context.config.c_int > -100)
        if (context.config.c_int > -100)
          CustomSchemaTransform_1(context, df_CustomReformat_2).interim(
            "graph",
            "UpGzVXAppKLjmFlZQm_9T$$2ZzAG_LqGdhuJmCtgrzR7",
            "Retv3ZlzCpZzkNzZzUYgH$$yhd50aaB33ODNAGqK_W7P"
          )
        else df_CustomReformat_2
      else null
    val df_CustomJoin_1 =
      if (context.config.c_int > -100)
        CustomJoin_1(context,
                     df_CustomSchemaTransform_1,
                     df_CustomSchemaTransform_1
        ).interim("graph",
                  "6wEX74G1sEN27pfw8rGHM$$trflJvoEIbF1T1-D6LGIo",
                  "LMX8eWpK0T3A5CcblpU8y$$2TWbfn1ZwdB1nTVAoFB6G"
        )
      else null
    if (df_CustomJoin_1 != null) {
      df_CustomJoin_1.cache().count()
      df_CustomJoin_1.unpersist()
    }
    if (context.config.c_int > -100 && context.config.c_int > -100) {
      withSubgraphName("graph", context.spark) {
        withTargetId("dest_test", context.spark) {
          dest_test(context, df_reformatted_columns)
        }
      }
    }
    val df_Reformat_12 = Reformat_12(context, df_schema_transform).interim(
      "graph",
      "SZN5-v9MafvGLy6gCWHUV$$Lg9sLnz9osw0CYf5oJ2s5",
      "k62LBfcGvM6fRriAMxLyy$$Xrcm4rTYYUoUoqGaRW28Z"
    )
    df_Reformat_12.cache().count()
    df_Reformat_12.unpersist()
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
    spark.conf.set("spark_config1",                  "spark_config_value_1")
    spark.conf.set("spark_config2",                  "spark_config_value_2")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SCALA_BASIC")
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config1", "hadoop_config_value1")
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config2",          "hadoop_config_value2")
    try MetricsCollector.start(spark, "pipelines/SCALA_BASIC", context.config)
    catch {
      case _: Throwable =>
        MetricsCollector.start(spark, "pipelines/SCALA_BASIC")
    }
    graph(context)
    MetricsCollector.end(spark)
  }

}
