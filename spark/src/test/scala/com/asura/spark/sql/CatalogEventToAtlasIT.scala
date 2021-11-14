package com.asura.spark.sql

import java.nio.file.Files
import scala.concurrent.duration._
import scala.language.postfixOps
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually._
import com.asura.spark.{Conf, TestUtils}
import com.asura.spark.sql.testhelper.BaseResourceIT
import com.asura.spark.types.internal
import com.asura.spark.types.metadata.{DB_TYPE_STRING, STORAGEDESC_TYPE_STRING, TABLE_TYPE_STRING}
import com.asura.spark.util.SparkUtils
import org.scalatest.Matchers.be


/**
 * @author asura7969
 * @create 2021-11-09-09:36
 */
class CatalogEventToAtlasIT extends BaseResourceIT with Matchers {
  import TestUtils._

  private var sparkSession: SparkSession = _

  private var processor: SparkCatalogEventProcessor = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local")
      .getOrCreate()
    processor =
      new SparkCatalogEventProcessor(asuraClient, new Conf())
    processor.startThread()
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    super.afterAll()
  }

  test("catalog spark db event to Atlas entities") {
    val dbName = uniqueName("db1")

    val tempDbPath = Files.createTempDirectory("db_")
    val dbDefinition = createDB(dbName, tempDbPath.normalize().toUri.toString)
    SparkUtils.getExternalCatalog().createDatabase(dbDefinition, ignoreIfExists = true)
    processor.pushEvent(CreateDatabaseEvent(dbName))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      val entity = getEntity(DB_TYPE_STRING, internal.sparkDbUniqueAttribute(dbName))
      entity should not be (null)
      entity.getAttribute("name") should be (dbName)
      entity.getAttribute("owner") should be (SparkUtils.currUser())
      entity.getAttribute("ownerType") should be ("USER")
    }

    SparkUtils.getExternalCatalog().dropDatabase(dbName, ignoreIfNotExists = true, cascade = false)
    processor.pushEvent(DropDatabaseEvent(dbName))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
        getEntity(DB_TYPE_STRING, internal.sparkDbUniqueAttribute(dbName))
    }
  }

  test("catalog spark table event to Atlas entities") {
    val dbName = uniqueName("db2")
    val tbl1Name = uniqueName("tbl1")
    val tbl2Name = uniqueName("tbl2")

    val tempDbPath = Files.createTempDirectory("db_")
    val dbDefinition = createDB(dbName, tempDbPath.normalize().toUri.toString)
    SparkUtils.getExternalCatalog().createDatabase(dbDefinition, ignoreIfExists = true)
    processor.pushEvent(CreateDatabaseEvent(dbName))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      val entity = getEntity(DB_TYPE_STRING, internal.sparkDbUniqueAttribute(dbName))
      entity should not be (null)
      entity.getAttribute("name") should be (dbName)
    }

    // Create new table
    val schema = new StructType()
      .add("user", StringType)
      .add("age", IntegerType)
    val sd = CatalogStorageFormat.empty
    val tableDefinition = createTable(dbName, tbl1Name, schema, sd)
    SparkUtils.getExternalCatalog().createTable(tableDefinition, ignoreIfExists = true)
    processor.pushEvent(CreateTableEvent(dbName, tbl1Name))

    Thread.sleep(5 * 1000)
    eventually(timeout(30 seconds), interval(1 seconds)) {
      val sdEntity = getEntity(STORAGEDESC_TYPE_STRING,
        internal.sparkStorageFormatUniqueAttribute(dbName, tbl1Name))
      sdEntity should not be (null)

      val tblEntity = getEntity(TABLE_TYPE_STRING,
        internal.sparkTableUniqueAttribute(dbName, tbl1Name))
      tblEntity should not be (null)
      tblEntity.getAttribute("name") should be (tbl1Name)
    }

    // Rename table
    SparkUtils.getExternalCatalog().renameTable(dbName, tbl1Name, tbl2Name)
    processor.pushEvent(RenameTableEvent(dbName, tbl1Name, tbl2Name))
    eventually(timeout(30 seconds), interval(1 seconds)) {
      val tblEntity = getEntity(TABLE_TYPE_STRING,
        internal.sparkTableUniqueAttribute(dbName, tbl2Name))
      tblEntity should not be (null)
      tblEntity.getAttribute("name") should be (tbl2Name)

      val sdEntity = getEntity(STORAGEDESC_TYPE_STRING,
        internal.sparkStorageFormatUniqueAttribute(dbName, tbl2Name))
      sdEntity should not be (null)
    }

    // Drop table
    val tblDef2 = SparkUtils.getExternalCatalog().getTable(dbName, tbl2Name)
    processor.pushEvent(DropTablePreEvent(dbName, tbl2Name))
    processor.pushEvent(DropTableEvent(dbName, tbl2Name))

    // sleeping 2 secs - we have to do this to ensure there's no call on deletion, unfortunately...
    Thread.sleep(2 * 1000)
    // deletion request should not be added
    val tblEntity = getEntity(TABLE_TYPE_STRING,
      internal.sparkTableUniqueAttribute(dbName, tbl2Name))
    tblEntity should not be (null)
  }
}
