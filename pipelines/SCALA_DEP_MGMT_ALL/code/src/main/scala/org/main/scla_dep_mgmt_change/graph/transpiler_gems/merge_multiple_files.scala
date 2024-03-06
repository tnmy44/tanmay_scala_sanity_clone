package org.main.scla_dep_mgmt_change.graph.transpiler_gems

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.udfs.PipelineInitCode._
import org.main.scla_dep_mgmt_change.udfs.UDFs._
import org.main.scla_dep_mgmt_change.graph.transpiler_gems.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object merge_multiple_files {

  def apply(context: Context, inDF: DataFrame): DataFrame = {
    val spark = context.spark
    import org.apache.spark.sql.ProphecyDataFrame
    import org.apache.spark.sql.functions._
    lazy val dfWithUniqueId = ProphecyDataFrame
      .extendedDataFrame(inDF)
      .zipWithIndex(0, 1, "tempId", spark)
    ProphecyDataFrame
      .extendedDataFrame(dfWithUniqueId)
      .mergeMultipleFileContentInDataFrame(
        dfWithUniqueId.select(col("tempId"),
                              lit(context.config.fileread1).as("fileName")
        ),
        spark,
        delimiter = ",",
        abinitioSchema = """record
  string(2) name;
end;""",
        readFormat = "csv",
        joinWithInputDataframe = false
      )
      .drop("tempId")
  }

}
