package org.main.scla_dep_mgmt_change.graph.Subgraph_4.config

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
  var path_var: String =
    "dbfs:/Prophecy/qa_data/csv/CustomersDatasetInputWithHeader.csv"
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
