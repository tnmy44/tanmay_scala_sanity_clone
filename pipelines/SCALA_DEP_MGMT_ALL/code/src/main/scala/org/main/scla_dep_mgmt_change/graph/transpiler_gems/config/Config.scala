package org.main.scla_dep_mgmt_change.graph.transpiler_gems.config

import org.apache.spark.sql._
import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

  implicit val interimOutput: InterimOutput = InterimOutputHive2("")
}

case class Config(
  var fileread1_sub: String =
    "dbfs:/Prophecy/qa_data/csv/all_type_no_partition",
  var fileread1: String = "dbfs:/Prophecy/qa_data/csv/all_type_no_partition",
  var basefile:  String = "dbfs:/tmp/sony/random/check1",
  var test56:    String = "dbfs:/Prophecy/qa_data/csv/all_type_no_partition"
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
