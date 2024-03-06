package org.main.scla_dep_mgmt

import io.prophecy.libs._
import org.main.scla_dep_mgmt.config._
import org.main.scla_dep_mgmt.config.ConfigStore.interimOutput
import org.main.scla_dep_mgmt.udfs.UDFs._
import org.main.scla_dep_mgmt.udfs.PipelineInitCode._
import org.main.scla_dep_mgmt.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def graph(context: Context): Unit = {
    if (context.config.c_int > -100) {
      val df_src_parquet_all_type_and_partition_withspacehyphens1_1 =
        src_parquet_all_type_and_partition_withspacehyphens1_1(context).interim(
          "graph",
          "s4ehKkFCetG2VgqVT803x$$SYJL4OsOSk9mqknMqJqwk",
          "t7BPgmG1WirWmwJOQIfjC$$h1uOt8MGrvn9GmqDg4dDN"
        )
      Lookup_1(context,
               df_src_parquet_all_type_and_partition_withspacehyphens1_1
      )
    }
    val df_src_parquet_all_type_and_partition_withspacehyphens1 =
      src_parquet_all_type_and_partition_withspacehyphens1(context).interim(
        "graph",
        "TwNShMrBLKTbLIfc-k4Av$$AT3IBjwitBQrt669MKiis",
        "tjA55_AYFEOCIYoaums3t$$2b2FrW1EI08Hyo8ehCcZj"
      )
    val df_Subgraph_1 = Subgraph_1.apply(
      Subgraph_1.config.Context(context.spark, context.config.Subgraph_1),
      df_src_parquet_all_type_and_partition_withspacehyphens1
    )
    val df_SchemaTransform_1 =
      SchemaTransform_1(context,
                        df_src_parquet_all_type_and_partition_withspacehyphens1
      ).interim("graph",
                "1l7pIm7USWNvX5X68oLOp$$putyM1Wx7f1QJRtUmp_mf",
                "hiKwRqmAib-xWxVKP9oko$$jG8nL892OttFQMdWEH-HG"
      )
    val df_Script_1 =
      if (context.config.c_int > -100)
        if (context.config.c_int > -100)
          Script_1(context,
                   df_src_parquet_all_type_and_partition_withspacehyphens1
          ).cache()
            .interim("graph",
                     "C6H1gUTW71hJAwO4m723A$$ccQvaggR4xHPCtSFi706z",
                     "xwGf5rqRxh_ryrTw2H8Fp$$eoENmiaDSVxIVFJSAZMZU"
            )
        else df_src_parquet_all_type_and_partition_withspacehyphens1
      else null
    val df_Reformat_4 = if (context.config.c_int > -100) {
      val df_Join_1 = Join_1(context, df_Script_1, df_SchemaTransform_1)
        .cache()
        .interim("graph",
                 "EYwDOxyz4R7cZIGVOvb6k$$iAKiuMtQ5fyb3KDfkXl_z",
                 "Lr_YfzMB_CeOGrnn1aI_K$$6Do1NtJv1q2uv0Kn_ezdQ"
        )
      Reformat_4(context, df_Join_1).interim(
        "graph",
        "IAkb7Sqtb2S500vMg-CLy$$HqUJaC0em2u2vCK3el27S",
        "U_N7JtdCQh8kc8WEW0qgx$$EANyIm_Bg5pKk9jGmIp7A"
      )
    } else
      null
    val df_Subgraph_3 = Subgraph_3.apply(
      Subgraph_3.config.Context(context.spark, context.config.Subgraph_3),
      df_Subgraph_1
    )
    val df_Reformat_7 = Reformat_7(context, df_Subgraph_3).interim(
      "graph",
      "TlsyTeAQmmEmCyDjA_07K$$YD_gKdZbl4MbPgwo680iZ",
      "Guy98FN7qdYerBZnrfunX$$vHIp3geSSR_OqWb8OZtQd"
    )
    val df_DONOT_DELETE =
      DONOT_DELETE(context,
                   df_src_parquet_all_type_and_partition_withspacehyphens1
      ).interim("graph",
                "PnmNEwcc3h-lbfQYT_ctj$$RYRFLZOBNT0obmK6QDqU3",
                "mPVUV2135EIh3ssMfRpXC$$LBAsUOYfaEb4LoDPvnsa3"
      )
    df_DONOT_DELETE.cache().count()
    df_DONOT_DELETE.unpersist()
    val df_Subgraph_4 = Subgraph_4
      .apply(
        Subgraph_4.config.Context(context.spark, context.config.Subgraph_4),
        df_Reformat_7
      )
      .cache()
    val df_Reformat_10 = Reformat_10(context, df_Subgraph_4).interim(
      "graph",
      "uQVSVC3aMR__yXnrHO331$$Y_h6Oc0tTi8_JrZDsISkc",
      "z-0xdqmEhlVsQst6Z9GJK$$_8K_oeZM4fuFb3vKNrf8Y"
    )
    df_Reformat_10.cache().count()
    df_Reformat_10.unpersist()
    if (df_Reformat_4 != null) {
      df_Reformat_4.cache().count()
      df_Reformat_4.unpersist()
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
    spark.conf.set("spark_config1", "spark_config1_value")
    spark.conf.set("spark_config2", "spark_config2_value")
    spark.conf
      .set("prophecy.metadata.pipeline.uri", "pipelines/DONOT_OPEN_SCALA")
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config1", "hadoop_config1_value")
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config2", "hadoop_config2_value")
    try MetricsCollector.start(spark,
                               "pipelines/DONOT_OPEN_SCALA",
                               context.config
    )
    catch {
      case _: Throwable =>
        MetricsCollector.start(spark, "pipelines/DONOT_OPEN_SCALA")
    }
    graph(context)
    MetricsCollector.end(spark)
  }

}
