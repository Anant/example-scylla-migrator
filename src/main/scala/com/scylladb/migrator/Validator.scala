package com.scylladb.migrator

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.ReadConf
import com.scylladb.migrator.config.{ MigratorConfig, SourceSettings, TargetSettings }
import com.scylladb.migrator.validation.RowComparisonFailure
import org.apache.log4j.{ Level, LogManager, Logger }
import org.apache.spark.sql.SparkSession

object Validator {
  val log = LogManager.getLogger("com.scylladb.migrator")

  def runValidation(config: MigratorConfig)(
    implicit spark: SparkSession): List[RowComparisonFailure] = {

    val keyspace = spark.conf.get("spark.validation.keyspace")    
    val tableName = spark.conf.get("spark.validation.table")

    val sourceSettings = config.source match {
      case s: SourceSettings.Cassandra => s
      case otherwise =>
        throw new RuntimeException(
          s"Validation only supports validating against Cassandra/Scylla " +
            s"(found ${otherwise.getClass.getSimpleName} settings)")
    }

    val targetSettings = config.target match {
      case s: TargetSettings.Astra => s
      case otherwise =>
        throw new RuntimeException(
          s"Validation only supports validating against Cassandra/Scylla " +
            s"(found ${otherwise.getClass.getSimpleName} settings)")

    }

    val sourceConnector: CassandraConnector =
      Connectors.sourceConnector(spark.sparkContext.getConf, sourceSettings)
    val targetConnector: CassandraConnector =
      Connectors.targetConnectorAstra(spark.sparkContext.getConf, targetSettings)

    val renameMap = config.renames.map(rename => rename.from -> rename.to).toMap
    val sourceTableDef =
      sourceConnector.withSessionDo(
        Schema.tableFromCassandra(_, keyspace, tableName))

    val source = {
      val regularColumnsProjection =
        sourceTableDef.regularColumns.flatMap { colDef =>
          val alias = renameMap.getOrElse(colDef.columnName, colDef.columnName)

          if (sourceSettings.preserveTimestamps)
            List(
              ColumnName(colDef.columnName, Some(alias)),
              WriteTime(colDef.columnName, Some(alias + "_writetime")),
              TTL(colDef.columnName, Some(alias + "_ttl"))
            )
          else List(ColumnName(colDef.columnName, Some(alias)))
        }

      val primaryKeyProjection =
        (sourceTableDef.partitionKey ++ sourceTableDef.clusteringColumns)
          .map(colDef => ColumnName(colDef.columnName, renameMap.get(colDef.columnName)))

      spark.sparkContext
        .cassandraTable(keyspace, tableName)
        .withConnector(sourceConnector)
        .withReadConf(
          ReadConf
            .fromSparkConf(spark.sparkContext.getConf)
            .copy(
              splitCount      = sourceSettings.splitCount,
              fetchSizeInRows = sourceSettings.fetchSize
            )
        )
        .select(primaryKeyProjection ++ regularColumnsProjection: _*)
    }

    val joined = {
      val regularColumnsProjection =
        sourceTableDef.regularColumns.flatMap { colDef =>
          val renamedColName = renameMap.getOrElse(colDef.columnName, colDef.columnName)

          if (sourceSettings.preserveTimestamps)
            List(
              ColumnName(renamedColName),
              WriteTime(renamedColName, Some(renamedColName + "_writetime")),
              TTL(renamedColName, Some(renamedColName + "_ttl"))
            )
          else List(ColumnName(renamedColName))
        }

      val primaryKeyProjection =
        (sourceTableDef.partitionKey ++ sourceTableDef.clusteringColumns)
          .map(colDef => ColumnName(renameMap.getOrElse(colDef.columnName, colDef.columnName)))

      val joinKey = (sourceTableDef.partitionKey ++ sourceTableDef.clusteringColumns)
        .map(colDef => ColumnName(renameMap.getOrElse(colDef.columnName, colDef.columnName)))

      source
        .leftJoinWithCassandraTable(
          keyspace,
          tableName,
          SomeColumns(primaryKeyProjection ++ regularColumnsProjection: _*),
          SomeColumns(joinKey: _*))
        .withConnector(targetConnector)
    }

    joined
      .flatMap {
        case (l, r) =>
          RowComparisonFailure.compareRows(
            l,
            r,
            config.validation.floatingPointTolerance,
            config.validation.timestampMsTolerance,
            config.validation.ttlToleranceMillis,
            config.validation.writetimeToleranceMillis,
            config.validation.compareTimestamps
          )
      }
      .take(config.validation.failuresToFetch)
      .toList
  }

  def main(args: Array[String]): Unit = {
    val bundle_path = args(0)
    val database = args(1)
    val username = args(2)
    val password = args(3)
    val config_path = args(4)
    implicit val spark = SparkSession
      .builder()
      .appName("scylla-validator")
      .config("spark.task.maxFailures", "1024")
      .config("spark.stage.maxConsecutiveAttempts", "60")
      .config("spark.files", bundle_path)
	    .config("spark.cassandra.connection.config.cloud.path", s"secure-connect-${database}.zip")
	    .config("spark.cassandra.auth.username", username)
	    .config("spark.cassandra.auth.password", password)
      .getOrCreate

    Logger.getRootLogger.setLevel(Level.WARN)
    log.setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(Level.INFO)
    Logger.getLogger("com.datastax.spark.connector.cql.CassandraConnector").setLevel(Level.INFO)


    val df = spark.read.option("wholetext", true).text(config_path)
    val configDataString = df.select("value").first.mkString
    val migratorConfig = MigratorConfig.loadFromConfig(configDataString)

    // val migratorConfig =
    //   // MigratorConfig.loadFrom(spark.conf.get("spark.scylla.config"))

    log.info(s"Loaded config: ${migratorConfig}")

    val failures = runValidation(migratorConfig)

    if (failures.isEmpty) log.info("No comparison failures found - enjoy your day!")
    else {
      log.error("Found the following comparison failures:")
      log.error(failures.mkString("\n"))
    }
  }
}
