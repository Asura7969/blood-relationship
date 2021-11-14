package com.asura.spark.sql

import com.asura.spark.{WithHiveSupport, WithRemoteHiveMetastoreServiceSupport}
import com.asura.spark.types.{external, internal}
import com.asura.spark.sql.testhelper.BaseHarvesterSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{CommandResultExec, LeafExecNode}
import org.apache.spark.sql.execution.command.{ExecutedCommandExec, LoadDataCommand}

import java.io.{FileOutputStream, PrintWriter}
import java.nio.file.Files
import scala.util.Random

/**
 * @author asura7969
 * @create 2021-11-06-16:50
 */
abstract class BaseLoadDataHarvesterSuite
  extends BaseHarvesterSuite {

  protected val sourceTblName = "source_" + Random.nextInt(100000)

  protected override def initializeTestEnvironment(): Unit = {
    prepareDatabase()

    _spark.sql(s"CREATE TABLE $sourceTblName (name string)")
  }

  override protected def cleanupTestEnvironment(): Unit = {
    cleanupDatabase()
  }

  test("LOAD DATA [LOCAL] INPATH path source") {
    val file = Files.createTempFile("input", ".txt").toFile
    val out = new PrintWriter(new FileOutputStream(file))
    out.write("a\nb\nc\nd\n")
    out.close()

    val qe = _spark.sql(s"LOAD DATA LOCAL INPATH '${file.getAbsolutePath}' " +
      s"OVERWRITE INTO  TABLE $sourceTblName").queryExecution
    val qd = QueryDetail(qe, 0L)
    val node = qe.sparkPlan.collect { case p: LeafExecNode => p }
    assert(node.size == 1)
//    val execNode = node.head.asInstanceOf[ExecutedCommandExec]
    val execNode = node.head.asInstanceOf[CommandResultExec].commandPhysicalPlan.asInstanceOf[ExecutedCommandExec]
    val entities = CommandsHarvester.LoadDataHarvester.harvest(
      execNode.cmd.asInstanceOf[LoadDataCommand], qd)

    entities.input.getAttribute("name") should be (file.getAbsolutePath.toLowerCase)
    assertTable(entities.output, _dbName, sourceTblName, _clusterName, _useSparkTable)

  }
}


class LoadDataHarvesterSuite
  extends BaseLoadDataHarvesterSuite
  with WithHiveSupport {

  override def beforeAll(): Unit = {
    super.beforeAll()
    initializeTestEnvironment()
  }

  override def afterAll(): Unit = {
    cleanupTestEnvironment()
    super.afterAll()
  }

  override def getSparkSession: SparkSession = sparkSession

  override def getDbName: String = "sac"

  override def expectSparkTableModels: Boolean = true
}

class LoadDataHarvesterWithRemoteHMSSuite
  extends BaseLoadDataHarvesterSuite
    with WithRemoteHiveMetastoreServiceSupport {

  override def beforeAll(): Unit = {
    super.beforeAll()
    initializeTestEnvironment()
  }

  override def afterAll(): Unit = {
    cleanupTestEnvironment()
    super.afterAll()
  }

  override def getSparkSession: SparkSession = sparkSession

  override def expectSparkTableModels: Boolean = false

  override def getDbName: String = dbName
}
