package io.prophecy.pipelines.livy_sc_airflow_os.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
