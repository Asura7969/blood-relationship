package com.asura.spark.sql

import com.asura.spark.Entity
import com.asura.spark.types.{external, internal}
import com.asura.spark.util.{Logging, SparkUtils}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{PersistedView, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, Project, ReplaceTableAsSelectStatement, View}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.{FileRelation, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableCommand, CreateTableCommand, CreateViewCommand, LoadDataCommand}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.{InsertIntoDataSourceCommand, LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.sources.BaseRelation
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods.parse

/**
 * @author asura7969
 * @create 2021-10-31-16:28
 */
object CommandsHarvester extends Logging {

  def prepareEntity(tableIdentifier: TableIdentifier): Entity = {
    val tableName = SparkUtils.getTableName(tableIdentifier)
    val dbName = SparkUtils.getDatabaseName(tableIdentifier)
    val tableDef = SparkUtils.getExternalCatalog().getTable(dbName, tableName)
    tableToEntity(tableDef)
  }

  def tableToEntity(
    tableDefinition: CatalogTable,
    mockDbDefinition: Option[CatalogDatabase] = None): Entity = {
    if (SparkUtils.usingRemoteMetastoreService()) {
      external.hiveTableToReference(tableDefinition, mockDbDefinition)
    } else {
      internal.sparkTableToEntity(tableDefinition, mockDbDefinition)
    }
  }

  object LoadDataHarvester extends Harvester[LoadDataCommand] {
    override def harvest(node: LoadDataCommand, qd: QueryDetail): Unit = {
      val sourceEntity = external.pathToEntity(node.path)

      val targetEntity = prepareEntity(node.table)
    }
  }

  object CreateViewHarvester extends Harvester[CreateViewCommand] {
    override def harvest(node: CreateViewCommand, qd: QueryDetail): Unit = {
      val child = node.plan.asInstanceOf[Project].child
      val sourceEntity = child match {
        case r: UnresolvedRelation =>
          val identifier = r.multipartIdentifier
          var dbName = ""
          var table = ""
          if (identifier.length == 2) {
            dbName = identifier(0)
            table = identifier(1)
          } else {
            table = identifier(0)
            dbName = SparkUtils.getCurrentDatabase
          }
          val tableDef = SparkUtils.getExternalCatalog().getTable(dbName, table)
          tableToEntity(tableDef)
        case _: OneRowRelation => Seq.empty
        case n =>
          logWarn(s"Unknown leaf node: $n")
          Seq.empty
      }
      val viewIdentifier = node.name
      val targetEntity = Seq(prepareEntity(viewIdentifier))
    }
  }

  // InsertIntoDataSourceHarvester
  object InsertIntoDataSourceHarvester extends Harvester[InsertIntoDataSourceCommand] {
    override def harvest(node: InsertIntoDataSourceCommand, qd: QueryDetail): Unit = {
      node.query
      val outputEntities = discoverInputsEntities(node.logicalRelation, qd.qe.executedPlan)

    }
  }
  object SaveIntoDataSourceHarvester extends Harvester[SaveIntoDataSourceCommand] {
    override def harvest(node: SaveIntoDataSourceCommand, qd: QueryDetail): Unit = {
      val inputEntities = discoverInputsEntities(node.query, qd.qe.executedPlan)
      val outputEntities = node match {
        case JDBCEntities(jdbcEntities) => Seq(jdbcEntities)
        // case SHCEntities(shcEntities) => Seq(shcEntities)
        // case KafkaEntities(kafkaEntities) => kafkaEntities
        case e =>
          logWarn(s"Missing output entities: $e")
          Seq.empty
      }
    }
  }

  object CreateDataSourceTableHarvester extends Harvester[CreateDataSourceTableCommand] {
    override def harvest(
        node: CreateDataSourceTableCommand,
        qd: QueryDetail): Unit = {
      // only have table entities
      Seq(tableToEntity(node.table))
    }
  }

  object CreateTableHarvester extends Harvester[CreateTableCommand] {
    override def harvest(
        node: CreateTableCommand,
        qd: QueryDetail): Unit = {
      Seq(tableToEntity(node.table))
    }
  }

  private def discoverInputsEntities(
    plan: LogicalPlan,
    executedPlan: SparkPlan): Entity = {

    val tChildren: Seq[LogicalPlan] = plan.collectLeaves()
    tChildren.flatMap {
      case r: HiveTableRelation => Seq(tableToEntity(r.tableMeta))
      case v: View => Seq(tableToEntity(v.desc))
      case LogicalRelation(fileRelation: FileRelation, _, catalogTable, _) =>
        catalogTable.map(tbl => Seq(tableToEntity(tbl))).getOrElse(
          fileRelation.inputFiles.flatMap(file => Seq(external.pathToEntity(file))).toSeq)
        // hbase-connector 不是spark内置连接器，先忽略
      //case SHCEntities(shcEntities) => Seq(shcEntities)
      case JDBCEntities(jdbcEntities) => Seq(jdbcEntities)
      // kafka 不考虑
      // case KafkaEntities(kafkaEntities) => kafkaEntities
      case e =>
        logWarn(s"Missing unknown leaf node: $e")
        Seq.empty
    }

    null

  }

  object SHCEntities {
    private val SHC_RELATION_CLASS_NAME =
      "org.apache.spark.sql.execution.datasources.hbase.HBaseRelation"

    private val RELATION_PROVIDER_CLASS_NAME =
      "org.apache.spark.sql.execution.datasources.hbase.DefaultSource"

    def unapply(plan: LogicalPlan): Option[Entity] = plan match {
      case l: LogicalRelation
        if l.relation.getClass.getCanonicalName.endsWith(SHC_RELATION_CLASS_NAME) =>
        val baseRelation = l.relation.asInstanceOf[BaseRelation]
        val options = baseRelation.getClass.getMethod("parameters")
          .invoke(baseRelation).asInstanceOf[Map[String, String]]
        getSHCEntity(options)
      case sids: SaveIntoDataSourceCommand
        if sids.dataSource.getClass.getCanonicalName.endsWith(RELATION_PROVIDER_CLASS_NAME) =>
        getSHCEntity(sids.options)
      case _ => None
    }

    def unapply(plan: SparkPlan): Option[Entity] = plan match {
      case r: RowDataSourceScanExec
        if r.relation.getClass.getCanonicalName.endsWith(SHC_RELATION_CLASS_NAME) =>
        val baseRelation = r.relation.asInstanceOf[BaseRelation]
        val options = baseRelation.getClass.getMethod("parameters")
          .invoke(baseRelation).asInstanceOf[Map[String, String]]
        getSHCEntity(options)
      case _ => None
    }

    def getSHCEntity(options: Map[String, String]): Option[Entity] = {
      if (options.getOrElse("catalog", "") != "") {
        val catalog = options("catalog")
        val jObj = parse(catalog).asInstanceOf[JObject]
        val map = jObj.values
        val tableMeta = map("table").asInstanceOf[Map[String, _]]
        // `asInstanceOf` is required. Otherwise, it fails compilation.
        val nSpace = tableMeta.getOrElse("namespace", "default").asInstanceOf[String]
        val tName = tableMeta("name").asInstanceOf[String]
        Some(external.hbaseTableToEntity(tName, nSpace))
      } else {
        None
      }
    }
  }

  object JDBCEntities {
    private val JDBC_RELATION_CLASS_NAME =
      "org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation"

    private val JDBC_PROVIDER_CLASS_NAME =
      "org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider"

    def unapply(plan: LogicalPlan): Option[Entity] = plan match {
      case l: LogicalRelation
        if l.relation.getClass.getCanonicalName.endsWith(JDBC_RELATION_CLASS_NAME) =>
        val baseRelation = l.relation.asInstanceOf[BaseRelation]
        val options = baseRelation.getClass.getMethod("jdbcOptions")
          .invoke(baseRelation).asInstanceOf[JDBCOptions].parameters
        Some(getJdbcEnity(options))
      case sids: SaveIntoDataSourceCommand
        if sids.dataSource.getClass.getCanonicalName.endsWith(JDBC_PROVIDER_CLASS_NAME) =>
        Some(getJdbcEnity(sids.options))
      case _ => None
    }

    def unapply(plan: SparkPlan): Option[Entity] = plan match {
      case r: RowDataSourceScanExec
        if r.relation.getClass.getCanonicalName.endsWith(JDBC_PROVIDER_CLASS_NAME) =>
        val baseRelation = r.relation.asInstanceOf[BaseRelation]
        val options = baseRelation.getClass.getMethod("jdbcOptions")
          .invoke(baseRelation).asInstanceOf[JDBCOptions].parameters
        Some(getJdbcEnity(options))
      case _ => None
    }

    private def getJdbcEnity(options: Map[String, String]): Entity = {
      val url = options.getOrElse("url", "")
      val tableName = options.getOrElse("dbtable", "")
      external.rdbmsTableToEntity(url, tableName)
    }
  }

}
