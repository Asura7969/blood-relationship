package com.asura.spark.sql.testhelper

import com.asura.spark.{Conf, Entity}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

/**
 * @author asura7969
 * @create 2021-11-06-16:51
 */
abstract class BaseHarvesterSuite
  extends FunSuite
    with Matchers
    with TableEntityValidator{

  protected def getSparkSession: SparkSession

  protected def getDbName: String

  protected def expectSparkTableModels: Boolean

  protected def initializeTestEnvironment(): Unit = {}

  protected def cleanupTestEnvironment(): Unit = {}

  private val clientConf: Conf = new Conf()
  protected val _clusterName: String = clientConf.get("", "")

  protected lazy val _spark: SparkSession = getSparkSession
  protected lazy val _dbName: String = getDbName
  protected lazy val _useSparkTable: Boolean = expectSparkTableModels

  protected def prepareDatabase(): Unit = {
    _spark.sql(s"DROP DATABASE IF EXISTS ${_dbName} Cascade")
    _spark.sql(s"CREATE DATABASE ${_dbName}")
    _spark.sql(s"USE ${_dbName}")
  }

  protected def cleanupDatabase(): Unit = {
    _spark.sql(s"DROP DATABASE IF EXISTS ${_dbName} Cascade")
  }

  protected def assertTable(ref: Entity, tableName: String): Unit = {
    assertTable(ref, _dbName, tableName, _clusterName, _useSparkTable)
  }

  protected def assertTableWithNamePrefix(ref: Entity, tblNamePrefix: String): Unit = {
    assertTableWithNamePrefix(ref, _dbName, tblNamePrefix, _clusterName, _useSparkTable)
  }
}
