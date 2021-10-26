package com.asura.spark

import com.asura.spark.sql.{QueryDetail, SparkCatalogEventProcessor, SparkExecutionPlanProcessor}
import com.asura.spark.util.Logging
import com.google.common.annotations.VisibleForTesting
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogEvent
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

/**
 * @author asura7969
 * @create 2021-10-25-21:35
 */
class SparkAtlasEventTracker(client: AsuraClient, conf: Conf)
  extends SparkListener with QueryExecutionListener with Logging {

  private val enabled: Boolean = Utils.isSacEnabled(conf)

  // Processor to handle DDL related events
  @VisibleForTesting
  private[atlas] val catalogEventTracker = new SparkCatalogEventProcessor(client, conf)
  catalogEventTracker.startThread()

  // Processor to handle DML related events
  private val executionPlanTracker = new SparkExecutionPlanProcessor(client, conf)
  executionPlanTracker.startThread()

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    if (!enabled) {
      return
    }

    // We only care about SQL related events.
    event match {
      case e: ExternalCatalogEvent => catalogEventTracker.pushEvent(e)
      case _ => // Ignore other events
    }
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    if (!enabled) {
      return
    }

    if (qe.logical.isStreaming) {
      // streaming query will be tracked via Flink
      return
    }

    val qd = QueryDetail.fromQueryExecutionListener(qe, durationNs)
    executionPlanTracker.pushEvent(qd)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {

  }
}
