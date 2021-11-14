package com.asura.spark.sql

import com.asura.spark.types.{external, internal}
import com.asura.spark.types.internal.{sparkDbToEntity, sparkDbUniqueAttribute, sparkStorageFormatUniqueAttribute, sparkTableToEntity, sparkTableToEntityForAlterTable, sparkTableUniqueAttribute}
import com.asura.spark.types.metadata.{DB_TYPE_STRING, STORAGEDESC_TYPE_STRING, TABLE_TYPE_STRING}
import com.asura.spark.util.{Logging, SparkUtils}
import com.asura.spark.{AbstractEventProcessor, AsuraClient, Conf, Entity}
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.catalog.{AlterDatabaseEvent, AlterTableEvent, CatalogDatabase, CreateDatabaseEvent, CreateDatabasePreEvent, CreateTableEvent, CreateTablePreEvent, DropDatabaseEvent, DropDatabasePreEvent, DropTableEvent, DropTablePreEvent, ExternalCatalogEvent, RenameTableEvent}

import scala.collection.mutable

/**
 * @author asura7969
 * @create 2021-10-25-21:41
 */
class SparkCatalogEventProcessor(client: AsuraClient, val conf: Conf)
  extends AbstractEventProcessor[ExternalCatalogEvent] with Logging {

  private val cachedObject = new mutable.WeakHashMap[String, Object]

  override protected def process(e: ExternalCatalogEvent): Unit = {

    e match {
      case CreateDatabasePreEvent(_) => // No-op

      case CreateDatabaseEvent(db) =>
        val dbDefinition = SparkUtils.getExternalCatalog().getDatabase(db)
        val entity = sparkDbToEntity(dbDefinition, SparkUtils.currUser())
        client.saveEntity(entity)
        logDebug(s"Created db entity $db")

      case DropDatabasePreEvent(db) =>
        try {
          cachedObject.put(sparkDbUniqueAttribute(db),
            SparkUtils.getExternalCatalog().getDatabase(db))
        } catch {
          case _: NoSuchDatabaseException =>
            logDebug(s"Spark already deleted the database: $db")
        }

      case DropDatabaseEvent(db) =>
        client.deleteEntity(DB_TYPE_STRING, internal.sparkDbUniqueAttribute(db))
        cachedObject.remove(sparkDbUniqueAttribute(db)).foreach { o =>
          val dbDef = o.asInstanceOf[CatalogDatabase]
          val path = dbDef.locationUri.toString
          val pathEntity = external.pathToEntity(path)

          client.deleteEntity(pathEntity.getTypeName, pathEntity.qualifiedName)
        }

        logDebug(s"Deleted db entity $db")

      case CreateTablePreEvent(_, _) => // No-op

      // TODO. We should also not create/alter view table in Atlas
      case CreateTableEvent(db, table) =>
        val tableDefinition = SparkUtils.getExternalCatalog().getTable(db, table)
        val tableEntity = sparkTableToEntity(tableDefinition)
        client.saveEntity(tableEntity)
        logDebug(s"Created table entity $table without columns")

      case DropTablePreEvent(_, _) => // No-op

      case DropTableEvent(db, table) =>
        logDebug(s"Can't handle drop table event since we don't have context information for " +
          s"table $table in db $db. Can't delete table entity and corresponding entities.")

      case RenameTableEvent(db, name, newName) =>
        // Update storageFormat's unique attribute
        val sdEntity = new Entity(STORAGEDESC_TYPE_STRING)
        sdEntity.setAttribute("qualifiedName",
          sparkStorageFormatUniqueAttribute(db, newName))
        client.updateEntity(STORAGEDESC_TYPE_STRING,
          sparkStorageFormatUniqueAttribute(db, name),
          sdEntity)

        // Update Table name and Table's unique attribute
        val tableEntity = new Entity(TABLE_TYPE_STRING)
        tableEntity.setAttribute("qualifiedName",
          sparkTableUniqueAttribute(db, newName))
        tableEntity.setAttribute("name", newName)
        client.updateEntity(TABLE_TYPE_STRING,
          sparkTableUniqueAttribute(db, name),
          tableEntity)

        logDebug(s"Rename table entity $name to $newName")

      case AlterDatabaseEvent(db) =>
        val dbDefinition = SparkUtils.getExternalCatalog().getDatabase(db)
        val dbEntity = sparkDbToEntity(dbDefinition, SparkUtils.currUser())
        client.saveEntity(dbEntity)
        logDebug(s"Updated DB properties")

      case AlterTableEvent(db, table, kind) =>
        val tableDefinition = SparkUtils.getExternalCatalog().getTable(db, table)
        kind match {
          case "table" =>
            val tableEntity = sparkTableToEntityForAlterTable(tableDefinition)
            client.saveEntity(tableEntity)
            logDebug(s"Updated table entity $table without columns")

          case "dataSchema" =>
            // We don't mind updating column
            logDebug("Detected updating of table schema but ignored: " +
              "column update will not be tracked here")

          case "stats" =>
            logDebug(s"Stats update will not be tracked here")

          case _ =>
          // No op.
        }

      case f =>
        logDebug(s"Drop unknown event $f")


    }

  }
}
