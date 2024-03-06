package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.udfs.PipelineInitCode._
import org.main.scla_dep_mgmt_change.udfs.UDFs._
import org.main.scla_dep_mgmt_change.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object custom_join_with_hints {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame
  ): DataFrame =
    in0
      .as("in0")
      .hint("broadcast")
      .join(in1.as("in1").hint("shuffle_hash"),
            col("in0.C_STRING") =!= col("in1.Crime_Rate").cast(StringType),
            "inner"
      )
      .join(in2.as("in2"),
            col("in1.Crime_Rate").cast(StringType) =!= col("in2.c_varchar"),
            "inner"
      )
      .select(
        col("in0.C_NUM").as("C_NUM"),
        col("in0.C_NUM10").as("C_NUM10"),
        col("in0.C_DEC").as("C_DEC"),
        col("in0.C_NUMERIC").as("C_NUMERIC"),
        col("in0.C_INT").as("C_INT"),
        col("in0.C_INTEGER").as("C_INTEGER"),
        col("in0.C_DOUBLE").as("C_DOUBLE"),
        col("in0.C_FLOAT").as("C_FLOAT"),
        col("in0.C_COUBLE_PRECISION").as("C_COUBLE_PRECISION"),
        col("in0.C_REAL").as("C_REAL"),
        col("in0.C_VARCHAR").as("C_VARCHAR"),
        col("in0.C_VARCHAR50").as("C_VARCHAR50"),
        col("in0.C_CHAR").as("C_CHAR"),
        col("in0.C_CHAR10").as("C_CHAR10"),
        col("in0.C_STRING").as("C_STRING"),
        col("in0.C_STRING20").as("C_STRING20"),
        col("in0.C_TEXT").as("C_TEXT"),
        col("in0.C_TEXT30").as("C_TEXT30"),
        col("in0.C_BINARY").as("C_BINARY"),
        col("in0.C_BINARY100").as("C_BINARY100"),
        col("in0.C_VARBINARY").as("C_VARBINARY"),
        col("in0.C_BOOL").as("C_BOOL"),
        col("in0.C_TIMESTAMP").as("C_TIMESTAMP"),
        col("in0.C_DATE").as("C_DATE"),
        col("in0.C_DATETIME").as("C_DATETIME"),
        col("in0.C_TIME").as("C_TIME"),
        col("in0.C_ARRAY").as("C_ARRAY"),
        col("in0.C_OBJECT").as("C_OBJECT"),
        col("in0.C_GEOGRAPHY").as("C_GEOGRAPHY"),
        col("in1.Y_Value_of_Occupied_Homes").as("Y_Value_of_Occupied_Homes"),
        col("in1.Crime_Rate").as("Crime_Rate"),
        col("in1.Residential_Land_Zone").as("Residential_Land_Zone"),
        col("in1.Non_retail_Business_acres").as("Non_retail_Business_acres"),
        col("in1.Charles_River").as("Charles_River"),
        col("in1.Nitric_Oxide").as("Nitric_Oxide"),
        col("in1.Average_Rooms").as("Average_Rooms"),
        col("in1.Owner_Occupied_Units").as("Owner_Occupied_Units"),
        col("in1.Distance_to_Employment_Centers")
          .as("Distance_to_Employment_Centers"),
        col("in1.Accessibility_to_Highways").as("Accessibility_to_Highways"),
        col("in1.Property_Tax_Rate").as("Property_Tax_Rate"),
        col("in1.Pupil_Teacher_Ratio").as("Pupil_Teacher_Ratio"),
        col("in1.Lower_Status").as("Lower_Status"),
        col("in2.title").as("title"),
        col("in0.*"),
        col("in1.*")
      )

}
