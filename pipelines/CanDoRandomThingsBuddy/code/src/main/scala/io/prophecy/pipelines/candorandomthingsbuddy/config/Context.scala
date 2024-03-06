package io.prophecy.pipelines.candorandomthingsbuddy.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
