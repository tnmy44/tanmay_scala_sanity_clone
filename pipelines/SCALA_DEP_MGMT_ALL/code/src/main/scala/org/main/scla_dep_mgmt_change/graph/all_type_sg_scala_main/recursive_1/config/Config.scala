package org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main.recursive_1.config

import org.apache.spark.sql._
import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession
import org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main.recursive_1.Subgraph_2_1.config.{
  Config => Subgraph_2_1_Config
}
import org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main.recursive_1.Subgraph_1.config.{
  Config => Subgraph_1_Config
}

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

  implicit val interimOutput: InterimOutput = InterimOutputHive2("")
}

case class Config(
  var Subgraph_2_1:              Subgraph_2_1_Config = Subgraph_2_1_Config(),
  var c_rec1_c_string:           String = "hello sir how are you $$ my son",
  var c_rec1_c_int:              Int = 2321,
  var c_rec1_c_boolean:          Boolean = false,
  var c_rec1_c_spark_expression: String = "concat('a', 'b', 'recursive_1')",
  var Subgraph_1:                Subgraph_1_Config = Subgraph_1_Config(),
  var JDBC_DATABASE:             String = "test_database"
) extends ConfigBase

object DatabricksSecret {

  implicit val myIntReader: ConfigReader[DatabricksSecret] =
    ConfigReader[String].map { s =>
      val Array(scope, key) = s.split(":")
      DatabricksSecret(scope, key)
    }

}

case class DatabricksSecret(var scope: String, var key: String) {

  override def toString: String = {
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    dbutils.secrets.get(scope = scope, key = key)
  }

}

case class Context(spark: SparkSession, config: Config)
