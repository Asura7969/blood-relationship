package com.asura.spark.types

import com.asura.spark.{Entity, EntityDependencies}
import com.asura.spark.util.{Logging, SparkUtils}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable}

import java.util.Date
import scala.collection.JavaConverters.asJavaIterableConverter
import scala.collection.mutable

/**
 * @author asura7969
 * @create 2021-10-30-15:31
 */
object internal extends Logging {

  val cachedObjects = new mutable.HashMap[String, Object]

  def sparkDbUniqueAttribute(db: String): String = SparkUtils.getUniqueQualifiedPrefix() + db

  def sparkDbToEntity(dbDefinition: CatalogDatabase,
                      owner: String): Entity = {
    val dbEntity = new Entity(metadata.DB_TYPE_STRING)

    dbEntity.setAttribute(
      "qualifiedName", sparkDbUniqueAttribute(dbDefinition.name))
    dbEntity.setAttribute("name", dbDefinition.name)
    dbEntity.setAttribute("description", dbDefinition.description)
    dbEntity.setAttribute("location", dbDefinition.locationUri.toString)
    dbEntity.setAttribute("parameters", dbDefinition.properties.asJava)
    dbEntity.setAttribute("owner", owner)
    dbEntity.setAttribute("ownerType", "USER")

    dbEntity
  }

  def sparkStorageFormatUniqueAttribute(db: String, table: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table.storageFormat"
  }

  def sparkStorageFormatToEntity(
    storageFormat: CatalogStorageFormat,
    db: String,
    table: String): Entity = {

    val sdEntity = new Entity(metadata.STORAGEDESC_TYPE_STRING)

    sdEntity.setAttribute("qualifiedName",
      sparkStorageFormatUniqueAttribute(db, table))
    storageFormat.locationUri.foreach(uri => sdEntity.setAttribute("location", uri.toString))
    storageFormat.inputFormat.foreach(sdEntity.setAttribute("inputFormat", _))
    storageFormat.outputFormat.foreach(sdEntity.setAttribute("outputFormat", _))
    storageFormat.serde.foreach(sdEntity.setAttribute("serde", _))
    sdEntity.setAttribute("compressed", storageFormat.compressed)
    sdEntity.setAttribute("parameters", storageFormat.properties.asJava)

    sdEntity
  }

  def sparkTableUniqueAttribute(db: String, table: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table"
  }

  def sparkTableToEntity(
    tblDefinition: CatalogTable,
    mockDbDefinition: Option[CatalogDatabase] = None): Entity = {

    val tableDefinition = SparkUtils.getCatalogTableIfExistent(tblDefinition)
    val db = SparkUtils.getDatabaseName(tableDefinition)
    val table = SparkUtils.getTableName(tableDefinition)
    val dbDefinition = mockDbDefinition
      .getOrElse(SparkUtils.getExternalCatalog().getDatabase(db))

    val dbEntity = sparkDbToEntity(dbDefinition, tableDefinition.owner)
    val sdEntity =
      sparkStorageFormatToEntity(tableDefinition.storage, db, table)

    val tblEntity = new Entity(metadata.TABLE_TYPE_STRING)

    tblEntity.setAttribute("qualifiedName",
      sparkTableUniqueAttribute(db, table))
    tblEntity.setAttribute("name", table)
    tblEntity.setAttribute("tableType", tableDefinition.tableType.name)
    tblEntity.setAttribute("schemaDesc", tableDefinition.schema.simpleString)
    tblEntity.setAttribute("provider", tableDefinition.provider.getOrElse(""))
    if (tableDefinition.tracksPartitionsInCatalog) {
      tblEntity.setAttribute("partitionProvider", "Catalog")
    }
    tblEntity.setAttribute("partitionColumnNames", tableDefinition.partitionColumnNames.asJava)
    tableDefinition.bucketSpec.foreach(
      b => tblEntity.setAttribute("bucketSpec", b.toLinkedHashMap.asJava))
    tblEntity.setAttribute("owner", tableDefinition.owner)
    tblEntity.setAttribute("ownerType", "USER")
    tblEntity.setAttribute("createTime", new Date(tableDefinition.createTime))
    tblEntity.setAttribute("parameters", tableDefinition.properties.asJava)
    tableDefinition.comment.foreach(tblEntity.setAttribute("comment", _))
    tblEntity.setAttribute("unsupportedFeatures", tableDefinition.unsupportedFeatures.asJava)

    tblEntity.setRelationshipAttribute("db", dbEntity)
    tblEntity.setRelationshipAttribute("sd", sdEntity)

    tblEntity
  }

  def sparkTableToEntityForAlterTable(tblDefinition: CatalogTable,
                                      mockDbDefinition: Option[CatalogDatabase] = None)
  : Entity = {
    val tableEntity = sparkTableToEntity(tblDefinition, mockDbDefinition)
//    val deps = tableEntity.dependencies
//
//    val dbEntity = deps.filter(_.typeName == metadata.DB_TYPE_STRING).head
//    val sdEntity = deps.filter(_.typeName == metadata.STORAGEDESC_TYPE_STRING).head
//
//    // override attribute with reference - Atlas should already have these entities
//    tableEntity.entity.setRelationshipAttribute("db", dbEntity.asObjectId)
//    tableEntity.entity.setRelationshipAttribute("sd", sdEntity.asObjectId)

    tableEntity
  }

  def sparkProcessUniqueAttribute(executionId: Long): String = {
    SparkUtils.sparkSession.sparkContext.applicationId + "." + executionId
  }

  def etlProcessToEntity(inputs: Entity,
                         outputs: Entity,
                         logMap: Map[String, String]): EntityDependencies = {
    val entity = new Entity(metadata.PROCESS_TYPE_STRING)

    val appId = SparkUtils.sparkSession.sparkContext.applicationId
    val appName = SparkUtils.sparkSession.sparkContext.appName match {
      case "Spark shell" => s"Spark Job + $appId"
      case default => default + s" $appId"
    }
    entity.setAttribute("qualifiedName", appId)
    entity.setAttribute("name", appName)
    entity.setAttribute("currUser", SparkUtils.currUser())

//    val inputObjIds = inputs.map(_.asObjectId).asJava
//    val outputObjIds = outputs.map(_.asObjectId).asJava
//    entity.setAttribute("inputs", inputObjIds)  // Dataset and Model entity
//    entity.setAttribute("outputs", outputObjIds)  // Dataset entity
    logMap.foreach { case (k, v) => entity.setAttribute(k, v)}

    new EntityDependencies(entity, inputs, outputs)
  }
}
