package com.asura.spark

import com.asura.spark.util.Logging

import java.util.concurrent.atomic.AtomicLong

/**
 * @author asura7969
 * @create 2021-10-26-21:30
 */
object Utils extends Logging {
  private val executionId = new AtomicLong(0L)

  def issueExecutionId(): Long = executionId.getAndIncrement()

  def isSacEnabled(conf: Conf): Boolean = {
    if (!conf.get(Conf.ASURA_SPARK_ENABLED).toBoolean) {
      logWarn("Spark Asura Connector is disabled.")
      false
    } else {
      true
    }
  }
}
