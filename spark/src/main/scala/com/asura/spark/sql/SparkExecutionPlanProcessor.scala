package com.asura.spark.sql

import com.asura.spark.sql.CommandsHarvester.WriteToDataSourceV2Harvester
import com.asura.spark.util.Logging
import com.asura.spark.{AbstractEventProcessor, AsuraClient, Conf, Utils}
import org.apache.spark.sql.catalyst.analysis.PersistedView
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateDataSourceTableCommand, CreateTableCommand, CreateTableLikeCommand, CreateViewCommand, DataWritingCommandExec, ExecutedCommandExec, InsertIntoDataSourceDirCommand, LoadDataCommand}
import org.apache.spark.sql.execution.datasources.{InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand, SaveIntoDataSourceCommand}
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec
import org.apache.spark.sql.execution.{LeafExecNode, QueryExecution, SparkPlan, UnionExec}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveDirCommand, InsertIntoHiveTable}
import org.apache.spark.sql.streaming.SinkProgress

import java.util.UUID

/**
 * @author asura7969
 * @create 2021-10-25-21:42
 */
case class QueryDetail(
    qe: QueryExecution,
    executionId: Long,
    query: Option[String] = None,
    sink: Option[SinkProgress] = None,
    queryId: Option[UUID] = None)

object QueryDetail {
  def fromQueryExecutionListener(qe: QueryExecution, durationNs: Long): QueryDetail = {
    QueryDetail(qe, Utils.issueExecutionId(), Option(SQLQuery.get()))
  }
}

class SparkExecutionPlanProcessor(client: AsuraClient, val conf: Conf)
  extends AbstractEventProcessor[QueryDetail] with Logging {

  override protected def process(qd: QueryDetail): Unit = {
    val sparkPlan: SparkPlan = qd.qe.sparkPlan

    val outNodes: Seq[SparkPlan] = sparkPlan.collect {
      case p: UnionExec => p.children
      case p: DataWritingCommandExec => Seq(p)
      case p: WriteToDataSourceV2Exec => Seq(p)
      case p: LeafExecNode => Seq(p)
    }.flatten

    outNodes.foreach {
      case r: ExecutedCommandExec =>
        r.cmd match {
          case c: LoadDataCommand =>
            // Case 1. LOAD DATA LOCAL INPATH (from local)
            // Case 2. LOAD DATA INPATH (from HDFS)
            logDebug(s"LOAD DATA [LOCAL] INPATH (${c.path}) ${c.table}")
            CommandsHarvester.LoadDataHarvester.harvest(c, qd)

          case c: CreateViewCommand =>
            c.viewType match {
              case PersistedView =>
                logDebug(s"CREATE VIEW AS SELECT query: ${qd.qe}")
                CommandsHarvester.CreateViewHarvester.harvest(c, qd)

              case _ => Seq.empty
            }

          case c: SaveIntoDataSourceCommand =>
            logDebug(s"DATA FRAME SAVE INTO DATA SOURCE: ${qd.qe}")
            CommandsHarvester.SaveIntoDataSourceHarvester.harvest(c, qd)

          case c: InsertIntoDataSourceCommand =>
            logDebug(s"DATA FRAME INSERT INTO DATA SOURCE: ${qd.qe}")
            CommandsHarvester.InsertIntoDataSourceHarvester.harvest(c, qd)

          case c: CreateTableCommand =>
            logDebug(s"CREATE TABLE USING external source - hive")
            CommandsHarvester.CreateTableHarvester.harvest(c, qd)

          /**
           * {{{
           *   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
           *   LIKE [other_db_name.]existing_table_name
           *   [USING provider |
           *    [
           *     [ROW FORMAT row_format]
           *     [STORED AS file_format] [WITH SERDEPROPERTIES (...)]
           *    ]
           *   ]
           *   [locationSpec]
           *   [TBLPROPERTIES (property_name=property_value, ...)]
           * }}}
           */
          case c: CreateTableLikeCommand =>
            logDebug(s"CREATE TABLE LIKE ...")
            CommandsHarvester.CreateTableLikeHarvester.harvest(c, qd)

          case c: CreateDataSourceTableCommand =>
            logDebug(s"CREATE TABLE USING external source")
            CommandsHarvester.CreateDataSourceTableHarvester.harvest(c, qd)

          /**
           * {{{
           *   INSERT OVERWRITE DIRECTORY (path=STRING)?
           *   USING format OPTIONS ([option1_name "option1_value", option2_name "option2_value", ...])
           *   SELECT ...
           * }}}
           */
          case c: InsertIntoDataSourceDirCommand =>
            logDebug(s"INSERT OVERWRITE DIRECTORY ...")
            CommandsHarvester.InsertIntoDataSourceDirHarvester.harvest(c, qd)

          case _ =>
            Seq.empty
        }

      case r: DataWritingCommandExec =>
        r.cmd match {
          case c: InsertIntoHiveTable =>
            logDebug(s"INSERT INTO HIVE TABLE query ${qd.qe}")
            CommandsHarvester.InsertIntoHiveTableHarvester.harvest(c, qd)

          case c: InsertIntoHadoopFsRelationCommand =>
            logDebug(s"INSERT INTO SPARK TABLE query ${qd.qe}")
            CommandsHarvester.InsertIntoHadoopFsRelationHarvester.harvest(c, qd)

          case c: CreateHiveTableAsSelectCommand =>
            logDebug(s"CREATE TABLE AS SELECT query: ${qd.qe}")
            CommandsHarvester.CreateHiveTableAsSelectHarvester.harvest(c, qd)

          case c: CreateDataSourceTableAsSelectCommand =>
            logDebug(s"CREATE TABLE USING xx AS SELECT query: ${qd.qe}")
            CommandsHarvester.CreateDataSourceTableAsSelectHarvester.harvest(c, qd)
          /**
           * {{{
           *   INSERT OVERWRITE [LOCAL] DIRECTORY
           *   path
           *   [ROW FORMAT row_format]
           *   [STORED AS file_format]
           *   SELECT ...
           * }}}
            */
          case c: InsertIntoHiveDirCommand =>
            logDebug(s"CREATE TABLE USING xx AS SELECT query: ${qd.qe}")
            CommandsHarvester.InsertIntoHiveDirHarvester.harvest(c, qd)

          case _ =>
            Seq.empty
        }

      case r: WriteToDataSourceV2Exec =>
        WriteToDataSourceV2Harvester.harvest(r, qd)

      case _ =>
        Seq.empty
    }

  }
}
