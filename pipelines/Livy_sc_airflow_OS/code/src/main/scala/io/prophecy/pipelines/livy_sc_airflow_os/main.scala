package io.prophecy.pipelines.livy_sc_airflow_os

import io.prophecy.libs._
import io.prophecy.pipelines.livy_sc_airflow_os.config._
import io.prophecy.pipelines.livy_sc_airflow_os.udfs.UDFs._
import io.prophecy.pipelines.livy_sc_airflow_os.udfs.PipelineInitCode._
import io.prophecy.pipelines.livy_sc_airflow_os.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_livy_src_dataset = livy_src_dataset(context)
    val df_sector_reformat  = sector_reformat(context, df_livy_src_dataset)
    val df_print_sector     = print_sector(context,    df_sector_reformat)
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
    spark.conf
      .set("prophecy.metadata.pipeline.uri", "pipelines/Livy_sc_airflow_OS")
    registerUDFs(spark)
    try MetricsCollector.start(spark,
                               "pipelines/Livy_sc_airflow_OS",
                               context.config
    )
    catch {
      case _: Throwable =>
        MetricsCollector.start(spark, "pipelines/Livy_sc_airflow_OS")
    }
    apply(context)
    MetricsCollector.end(spark)
  }

}
