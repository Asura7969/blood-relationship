package com.asura.spark.types

import com.asura.spark.Entity
import com.asura.spark.Entity.REFERENCEABLE_ATTRIBUTE_NAME
import com.asura.spark.util.{JdbcUtils, SparkUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable}

import java.io.File
import java.net.URI

/**
 * @author asura7969
 * @create 2021-10-30-15:28
 */
object external {

  val FS_PATH_TYPE_STRING = "fs_path"
  val HDFS_PATH_TYPE_STRING = "hdfs_path"

  def pathToEntity(path: String): Entity = {
    val uri = resolveURI(path)
    val fsPath = new Path(uri)
    if (uri.getScheme == "hdfs") {
      val entity = new Entity(HDFS_PATH_TYPE_STRING)
      entity.setAttribute("name",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("path",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute(REFERENCEABLE_ATTRIBUTE_NAME, uri.toString)
      entity.setAttribute("clusterName", uri.getAuthority)

      entity
    } else {
      val entity = new Entity(FS_PATH_TYPE_STRING)
      entity.setAttribute("name",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("path",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute(REFERENCEABLE_ATTRIBUTE_NAME, uri.toString)

      entity
    }
  }

  private def qualifiedPath(path: String): Path = {
    val p = new Path(path)
    val fs = p.getFileSystem(SparkUtils.sparkSession.sparkContext.hadoopConfiguration)
    p.makeQualified(fs.getUri, fs.getWorkingDirectory)
  }

  private def resolveURI(path: String): URI = {
    val uri = new URI(path)
    if (uri.getScheme() != null) {
      return uri
    }

    val qUri = qualifiedPath(path).toUri

    // Path.makeQualified always converts the path as absolute path, but for file scheme
    // it provides different prefix.
    // (It provides prefix as "file" instead of "file://", which both are actually valid.)
    // Given we have been providing it as "file://", the logic below changes the scheme of
    // type "file" to "file://".
    if (qUri.getScheme == "file") {
      // make sure to handle if the path has a fragment (applies to yarn
      // distributed cache)
      if (qUri.getFragment() != null) {
        val absoluteURI = new File(qUri.getPath()).getAbsoluteFile().toURI()
        new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
          uri.getFragment())
      } else {
        new File(path).getAbsoluteFile().toURI()
      }
    } else {
      qUri
    }
  }

  // ================ HBase entities ======================
  val HBASE_NAMESPACE_STRING = "hbase_namespace"
  val HBASE_TABLE_STRING = "hbase_table"
  val HBASE_COLUMNFAMILY_STRING = "hbase_column_family"
  val HBASE_COLUMN_STRING = "hbase_column"
  val HBASE_TABLE_QUALIFIED_NAME_FORMAT = "%s:%s@%s"

  def hbaseTableToEntity(tableName: String, nameSpace: String): Entity = {
    val hbaseEntity = new Entity(HBASE_TABLE_STRING)
    hbaseEntity.setAttribute("qualifiedName",
      getTableQualifiedName(nameSpace, tableName))
    hbaseEntity.setAttribute("name", tableName.toLowerCase)
    hbaseEntity.setAttribute("uri", nameSpace.toLowerCase + ":" + tableName.toLowerCase)

    hbaseEntity
  }

  private def getTableQualifiedName(
                                     nameSpace: String,
                                     tableName: String): String = {
    if (nameSpace == null || tableName == null) {
      null
    } else {
      String.format(HBASE_TABLE_QUALIFIED_NAME_FORMAT, nameSpace.toLowerCase,
        tableName.toLowerCase.substring(tableName.toLowerCase.indexOf(":") + 1))
    }
  }

  // ================ RDBMS based entities ======================
  val RDBMS_TABLE = "rdbms_table"

  /**
   * Converts JDBC RDBMS properties into Atlas entity
   *
   * @param url
   * @param tableName
   * @return
   */
  def rdbmsTableToEntity(url: String, tableName: String): Entity = {
    val jdbcEntity = new Entity(RDBMS_TABLE)

    val databaseName = JdbcUtils.getDatabaseName(url)
    jdbcEntity.setAttribute("qualifiedName", getRdbmsQualifiedName(databaseName, tableName))
    jdbcEntity.setAttribute("name", tableName)

    jdbcEntity
  }

  /**
   * Constructs the the full qualified name of the databse
   *
   * @param databaseName
   * @param tableName
   * @return
   */
  private def getRdbmsQualifiedName(databaseName: String, tableName: String): String =
    s"${databaseName.toLowerCase}.${tableName.toLowerCase}"

  // ================== Hive Catalog entities =====================
  val HIVE_TABLE_TYPE_STRING = "hive_table"

  // scalastyle:off
  /**
   * This is based on the logic how Hive Hook defines qualifiedName for Hive DB (borrowed from Apache Atlas v1.1).
   * https://github.com/apache/atlas/blob/release-1.1.0-rc2/addons/hive-bridge/src/main/java/org/apache/atlas/hive/bridge/HiveMetaStoreBridge.java#L833-L841
   *
   * As we cannot guarantee same qualifiedName for temporary table, we just don't support
   * temporary table in SAC.
   */
  // scalastyle:on
  def hiveTableUniqueAttribute(
                                db: String,
                                table: String): String = {
    s"${db.toLowerCase}.${table.toLowerCase}"
  }

  def hiveTableToReference(
                            tblDefinition: CatalogTable,
                            mockDbDefinition: Option[CatalogDatabase] = None): Entity = {
    val tableDefinition = SparkUtils.getCatalogTableIfExistent(tblDefinition)
    val db = SparkUtils.getDatabaseName(tableDefinition)
    val table = SparkUtils.getTableName(tableDefinition)
    hiveTableToReference(db, table)
  }

  def hiveTableToReference(db: String, table: String): Entity = {
    val qualifiedName = hiveTableUniqueAttribute(db, table)
    val entity = new Entity(HIVE_TABLE_TYPE_STRING)
    entity.setAttribute("qualifiedName", qualifiedName)
    entity
  }
}
