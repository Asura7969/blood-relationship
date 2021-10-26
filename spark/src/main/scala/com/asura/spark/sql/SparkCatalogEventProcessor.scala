package com.asura.spark.sql

import com.asura.spark.util.Logging
import com.asura.spark.{AbstractEventProcessor, AsuraClient, Conf}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogEvent

/**
 * @author asura7969
 * @create 2021-10-25-21:41
 */
class SparkCatalogEventProcessor(client: AsuraClient, val conf: Conf)
  extends AbstractEventProcessor[ExternalCatalogEvent] with Logging {

  override protected def process(e: ExternalCatalogEvent): Unit = {

  }
}
