package com.asura.spark.sql

import com.asura.spark.util.Logging
import com.asura.spark.{AbstractEventProcessor, AsuraClient, Conf, Utils}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.streaming.SinkProgress
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent

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

  override protected def process(e: QueryDetail): Unit = {

  }
}
