package com.asura.spark.sql

/**
 * @author asura7969
 * @create 2021-10-26-21:29
 */
class SparkExtension {

}

object SQLQuery {
  private[this] val sqlQuery = new ThreadLocal[String]
  def get(): String = sqlQuery.get
  def set(s: String): Unit = sqlQuery.set(s)
}
