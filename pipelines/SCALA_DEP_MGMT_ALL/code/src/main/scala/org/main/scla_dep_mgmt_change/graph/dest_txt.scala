package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object dest_txt {

  def apply(context: Context, in: DataFrame): Unit = {
    var writer = in.write.format("text").mode("overwrite")
    writer = writer
    writer = writer.option("lineSep", """
""")
    writer.save("dbfs:/tmp/e2e/sanity/scala/textdest1")
  }

}
