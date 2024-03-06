package org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main.config

import org.apache.spark.sql._
import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession
import org.main.scla_dep_mgmt_change.graph.all_type_sg_scala_main.recursive_1.config.{
  Config => recursive_1_Config
}

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

  implicit val interimOutput: InterimOutput = InterimOutputHive2("")
}

case class Config(
  var recursive_1:    recursive_1_Config = recursive_1_Config(),
  var c_sg1_c_string: String = "this is a test",
  var c_sg1_c_bool:   Boolean = true,
  var c_sg1_c_double: Double = 1.23214123e8d,
  var c_sg1_c_float:  Float = 2343.4233f,
  var c_sg1_c_int:    Int = 12312,
  var c_sg1_c_long:   Long = 23432423423L,
  var c_sg1_c_short:  Short = 21,
  var c_sg1_c_array_string: List[String] =
    List("value1this is not $$ son $$$$#-1(2)", "value2$$this is son"),
  var c_sg1_c_record: C_sg1_c_record = C_sg1_c_record(),
  var c_sg1_c_db_secrets: DatabricksSecret =
    DatabricksSecret(scope = "qasecrets_mysql", key = "username"),
  var c_sg1_c_spark_expression: String = "concat('a', 'b', 'c', 'd')",
  var c_sg1_c_limit:            Int = 15,
  var c_sg1_c_orderby: String =
    "concat(`c- short`, `c -  boolean _  `, `c-string`, `c  - int`)",
  var c_sg1_c_rowdistributor:    String = "`c- short` > -10",
  var c_sg1_join_expr_timestamp: String = "in0.`c_timestamp  __ for--today`",
  var c_sg1_aggregate:           String = "first(`- c long`)",
  var CSV_DATASET_LOCATION: String =
    "dbfs:/Prophecy/qa_data/csv/CustomersDatasetInputWithHeader.csv",
  var CATALOG_DATABASE:  String = "qa_database",
  var CATALOG_TABLENAME: String = "all_type_non_partitioned"
) extends ConfigBase

object C_sg1_c_record {

  implicit val confHint: ProductHint[C_sg1_c_record] =
    ProductHint[C_sg1_c_record](ConfigFieldMapping(CamelCase, CamelCase))

}

case class C_sg1_c_record(
  var c_sg1_c_record_c_bool:             Boolean = false,
  var c_sg1_c_record_c_string:           String = "this is another $$ value some-1(1)",
  var c_sg1_c_record_c_spark_expression: String = "concat('a', 'b')",
  var c_sg1_c_record_c_db_secrets: DatabricksSecret =
    DatabricksSecret(scope = "qasecrets_mysql", key = "username")
)

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
