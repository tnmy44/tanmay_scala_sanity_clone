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

object log_generation {

  case class LogProperties(
    componentName:          String = "",
    subComponentName:       String = "",
    perRowEventColumns:     List[PerRowEvents] = Nil,
    finalLogEventType:      Option[org.apache.spark.sql.Column] = None,
    finalLogEventText:      Option[org.apache.spark.sql.Column] = None,
    finalEventExtraColumns: List[(String, org.apache.spark.sql.Column)] = Nil,
    loggerType:             String = "General"
  )

  case class PerRowEvents(
    perRowEventType: org.apache.spark.sql.Column,
    perRowEventText: org.apache.spark.sql.Column
  )

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val inDFs  = List(in0)
    val spark  = context.spark
    val Config = context.config
    val props = LogProperties(
      componentName = "merge_multiple_files_1",
      subComponentName = "",
      perRowEventColumns = List(
        PerRowEvents(perRowEventType = lit("start"),
                     perRowEventText =
                       concat(lit("starting logging for "), col("seq"))
        )
      ),
      finalLogEventType = Some(lit("finish")),
      finalLogEventText =
        Some(concat(lit("ending log for name: "), col("seq"))),
      finalEventExtraColumns = List(),
      loggerType = "General"
    )
    import org.apache.spark.sql.ProphecyDataFrame
    import org.apache.spark.sql.functions._
    val dfToLog = props.loggerType match {
      case "Rollup" =>
        inDFs.last
      case _ =>
        inDFs.head
    }
    val inputCount = props.loggerType match {
      case "Join" =>
        inDFs.foldLeft(0L)((x, y) => x + y.count())
      case "AssignKeys" =>
        inDFs.take(2).foldLeft(0L)((x, y) => x + y.count())
      case "Rollup" =>
        inDFs.last.count()
      case _ =>
        inDFs.head.count()
    }
    val outputCount = props.loggerType match {
      case "AssignKeys" =>
        inDFs.takeRight(3).foldLeft(0L)((x, y) => x + y.count())
      case _ =>
        0
    }
    val (perRowEventTypes, perRowEventTexts) = props.perRowEventColumns match {
      case Nil =>
        (None, None)
      case eventCols =>
        val (typeVals, textVals) =
          eventCols.map(p => (p.perRowEventType, p.perRowEventText)).unzip
        (Some(array(typeVals: _*)), Some(array(textVals: _*)))
    }
    val fEventType = props.finalLogEventType match {
      case None =>
        None
      case Some(e) =>
        Some(e)
    }
    val fEventText = props.finalLogEventText match {
      case None =>
        None
      case Some(e) =>
        Some(e)
    }
    ProphecyDataFrame
      .extendedDataFrame(dfToLog)
      .generateLogOutput(
        props.componentName,
        props.subComponentName,
        perRowEventTypes,
        perRowEventTexts,
        inputCount,
        Some(outputCount),
        fEventType,
        fEventText,
        props.finalEventExtraColumns.map(s => (s._1, s._2)).toMap,
        sparkSession = spark
      )
  }

}
