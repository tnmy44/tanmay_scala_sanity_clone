package org.main.scla_dep_mgmt_change.graph.Subgraph_3.config

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

case class Config(var subgraph_config: String = "subgraph_value_in_subgraph")
    extends ConfigBase

case class Context(spark: SparkSession, config: Config)
