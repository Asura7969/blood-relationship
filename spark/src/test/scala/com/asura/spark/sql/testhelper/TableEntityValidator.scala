package com.asura.spark.sql.testhelper

import com.asura.spark.Entity
import com.asura.spark.Entity.REFERENCEABLE_ATTRIBUTE_NAME
import com.asura.spark.types.{external, metadata}
import org.scalatest.FunSuite

/**
 * @author asura7969
 * @create 2021-11-07-14:50
 */
trait TableEntityValidator extends FunSuite {
  def assertTable(ref: Entity,
                  dbName: String,
                  tblName: String,
                  clusterName: String,
                  useSparkTable: Boolean): Unit = {
    if (useSparkTable) {
      assertSparkTable(ref, dbName, tblName)
    } else {
      assertHiveTable(ref, dbName, tblName, clusterName)
    }
  }

  def assertTableWithNamePrefix(ref: Entity,
                                dbName: String,
                                tblNamePrefix: String,
                                clusterName: String,
                                useSparkTable: Boolean): Unit = {
    if (useSparkTable) {
      assertSparkTableWithNamePrefix(ref, dbName, tblNamePrefix)
    } else {
      assertHiveTableWithNamePrefix(ref, dbName, tblNamePrefix, clusterName)
    }
  }

  def assertSparkTable(entity: Entity, dbName: String, tblName: String): Unit = {
    assert(entity.getTypeName === metadata.TABLE_TYPE_STRING)
    assert(entity.getAttribute("name") === tblName)
    assert(entity.getAttribute(REFERENCEABLE_ATTRIBUTE_NAME).toString
      .endsWith(s"$dbName.$tblName"))
  }

  def assertSparkTableWithNamePrefix(
                                      entity: Entity,
                                      dbName: String,
                                      tblNamePrefix: String): Unit = {
    assert(entity.getTypeName === metadata.TABLE_TYPE_STRING)
    assert(entity.getAttribute("name").toString.startsWith(tblNamePrefix))
    assert(entity.getAttribute(REFERENCEABLE_ATTRIBUTE_NAME).toString
      .contains(s"$dbName.$tblNamePrefix"))
  }

  def assertHiveTable(outputRef: Entity,
                      dbName: String,
                      tblName: String,
                      clusterName: String): Unit = {
    assert(outputRef.getTypeName === external.HIVE_TABLE_TYPE_STRING)
    assert(outputRef.qualifiedName === s"$dbName.$tblName@$clusterName")
  }

  def assertHiveTableWithNamePrefix(outputRef: Entity,
                                    dbName: String,
                                    tblNamePrefix: String,
                                    clusterName: String): Unit = {
    assert(outputRef.getTypeName === external.HIVE_TABLE_TYPE_STRING)
    assert(outputRef.qualifiedName.startsWith(s"$dbName.$tblNamePrefix"))
    assert(outputRef.qualifiedName.endsWith(s"@$clusterName"))
  }
}
