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

object write_to_multiple_files {

  def apply(context: Context, inDF: DataFrame): Unit = {
    import org.apache.spark.sql.ProphecyDataFrame
    ProphecyDataFrame
      .extendedDataFrame(
        inDF.withColumn("fileName", lit("dbfs:/tmp/sony/random"))
      )
      .breakAndWriteDataFrameForOutputFileWithSchema("""record
  integer(4) seq;
end;""", "fileName", "fixedFormat", Some(","))
  }

}
