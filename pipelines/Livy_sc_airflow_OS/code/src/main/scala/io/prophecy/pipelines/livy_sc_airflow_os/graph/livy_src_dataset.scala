package io.prophecy.pipelines.livy_sc_airflow_os.graph

import io.prophecy.libs._
import io.prophecy.pipelines.livy_sc_airflow_os.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object livy_src_dataset {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .schema(
        StructType(
          Array(
            StructField("Sector",                          StringType, true),
            StructField("Year",                            StringType, true),
            StructField("Month",                           StringType, true),
            StructField("Cereal and Products",             StringType, true),
            StructField("Pulses and Pulse Products",       StringType, true),
            StructField("Oils and Fats",                   StringType, true),
            StructField("Meat - Fish etc",                 StringType, true),
            StructField("Milk and Milk Products",          StringType, true),
            StructField("Spices and Condiments",           StringType, true),
            StructField("Vegetables",                      StringType, true),
            StructField("Fruits",                          StringType, true),
            StructField("Sugar - Honey etc",               StringType, true),
            StructField("Non Alcoholic Beverages",         StringType, true),
            StructField("Prepared Meals",                  StringType, true),
            StructField("Pan - Supari etc",                StringType, true),
            StructField("Food - beverages and tobacco ",   StringType, true),
            StructField("Fuel and Lighting",               StringType, true),
            StructField("Housing",                         StringType, true),
            StructField("Clothing and Bedding",            StringType, true),
            StructField("Footwear",                        StringType, true),
            StructField("Clothing - bedding and footwear", StringType, true),
            StructField("Medical Care",                    StringType, true),
            StructField("Education",                       StringType, true),
            StructField("Recreation and Amusement",        StringType, true),
            StructField("Transport and Communication",     StringType, true),
            StructField("Personal Care Items",             StringType, true),
            StructField("Household Requisites",            StringType, true),
            StructField("Others",                          StringType, true),
            StructField("Miscellaneous",                   StringType, true),
            StructField("General index",                   StringType, true),
            StructField("_c30",                            StringType, true)
          )
        )
      )
      .load(
        "hdfs://ip-10-0-0-91.us-west-2.compute.internal:8020/user/hadoop/data/csv/All_India_Index-February2016.csv"
      )

}
