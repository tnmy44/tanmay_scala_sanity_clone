package io.prophecy.pipelines.candorandomthingsbuddy.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  test56:        String = "dsfsaf",
  fileread1:     String = "dbfs:/Prophecy/qa_data/csv/all_type_no_partition",
  basefile:      String = "dbfs:/tmp/sony/random/check11",
  JDBC_DATABASE: String = "test_database"
) extends ConfigBase
