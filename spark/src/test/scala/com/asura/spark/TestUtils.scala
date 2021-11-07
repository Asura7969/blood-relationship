package com.asura.spark

import com.asura.spark.util.SparkUtils
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.types.StructType

import java.net.URI

/**
 * @author asura7969
 * @create 2021-11-06-16:46
 */
object TestUtils {
  def createDB(name: String, location: String): CatalogDatabase = {
    CatalogDatabase(name, "", new URI(location), Map.empty)
  }

  def createStorageFormat(
      locationUri: Option[URI] = None,
      inputFormat: Option[String] = None,
      outputFormat: Option[String] = None,
      serd: Option[String] = None,
      compressed: Boolean = false,
      properties: Map[String, String] = Map.empty): CatalogStorageFormat = {
    CatalogStorageFormat(locationUri, inputFormat, outputFormat, serd, compressed, properties)
  }

  def createTable(
      db: String,
      table: String,
      schema: StructType,
      storage: CatalogStorageFormat,
      isHiveTable: Boolean = false): CatalogTable = {
    CatalogTable(
      TableIdentifier(table, Some(db)),
      CatalogTableType.MANAGED,
      storage,
      schema,
      provider = if (isHiveTable) Some("hive") else None,
      bucketSpec = None,
      owner = SparkUtils.currUser())
  }

  def assertSubsetOf[T](set: Set[T], subset: Set[T]): Unit = {
    assert(subset.subsetOf(set), s"$subset is not a subset of $set")
  }
}
