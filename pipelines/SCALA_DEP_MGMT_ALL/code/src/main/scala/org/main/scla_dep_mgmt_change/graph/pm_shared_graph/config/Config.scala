package org.main.scla_dep_mgmt_change.graph.pm_shared_graph.config

import org.apache.spark.sql._
import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession
import org.main.scla_dep_mgmt_change.graph.pm_shared_graph.Subgraph_1.config.{
  Config => Subgraph_1_Config
}

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

  implicit val interimOutput: InterimOutput = InterimOutputHive2("")
}

case class Config(
  var Subgraph_1:    Subgraph_1_Config = Subgraph_1_Config(),
  var var1:          Int = 20,
  var JDBC_DATABASE: String = "test_database",
  var CSV_DATASET_LOCATION: String =
    "dbfs:/Prophecy/qa_data/csv/CustomersDatasetInputWithHeader.csv"
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
