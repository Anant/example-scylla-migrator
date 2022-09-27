package com.scylladb.migrator

import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Paths }
import java.util.concurrent.{ ScheduledThreadPoolExecutor, TimeUnit }
import com.datastax.spark.connector.rdd.partitioner.{ CassandraPartition, CqlTokenRange }
import com.datastax.spark.connector.rdd.partitioner.dht.Token
import com.datastax.spark.connector.writer._
import com.scylladb.migrator.config._
import org.apache.log4j.{ Level, LogManager, Logger }
import org.apache.spark.sql._
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import sun.misc.{ Signal, SignalHandler }
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.control.NonFatal

object Migrator {
  val log = LogManager.getLogger("com.scylladb.migrator")

  def main(args: Array[String]): Unit = {
    val bundle_path = args(0)
    val database = args(1)
    val username = args(2)
    val password = args(3)
    val config_path = args(4)
    // val conf = new SparkConf().setAppName("scylla-migrator")
    // val sc = new SparkContext(conf)
    implicit val spark = SparkSession
      .builder()
      .appName("scylla-migrator")
      .config("spark.task.maxFailures", "1024")
      .config("spark.stage.maxConsecutiveAttempts", "60")
      // .config("spark.files", bundle_path)
      .config("spark.files", s"${bundle_path},${config_path}")
      // .config("spark.files", config_path)
      .config("spark.cassandra.connection.config.cloud.path", s"secure-connect-${database}.zip")
      .config("spark.cassandra.auth.username", username)
      .config("spark.cassandra.auth.password", password)
      // .config("spark.scylla.config", config_path)
      .getOrCreate

    // println(spark.conf.get("spark.files"))

    // sc.addFile(bundle_path)
    // sc.addFile(config_path)

    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(5))

    Logger.getRootLogger.setLevel(Level.WARN)
    log.setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(Level.WARN)
    Logger.getLogger("com.datastax.spark.connector.cql.CassandraConnector").setLevel(Level.WARN)

    val migratorConfig =
      MigratorConfig.loadFrom(spark.conf.get("spark.scylla.config"))

    // migratorConfig.target match {
    //   case target: TargetSettings.Astra =>
    //     // spark.conf.set("spark.files", target.bundlePath)
    //     // sc.addFile(target.bundlePath)
    //     // spark.sparkContext.addFile(target.bundlePath)
    //     spark.conf.set("spark.cassandra.connection.config.cloud.path", s"secure-connect-${target.database}.zip")
    //     target.credentials match {
    //       case Credentials(username, password) => {        
    //         spark.conf.set("spark.cassandra.auth.username", username)
    //         spark.conf.set("spark.cassandra.auth.password", password)
    //       }
    //     }
    //   }

    log.info(s"Loaded config: ${migratorConfig}")

    val scheduler = new ScheduledThreadPoolExecutor(1)

    val sourceDF =
      migratorConfig.source match {
        case cassandraSource: SourceSettings.Cassandra =>
          readers.Cassandra.readDataframe(
            spark,
            cassandraSource,
            cassandraSource.preserveTimestamps,
            migratorConfig.skipTokenRanges)
      }

    log.info("Created source dataframe; resulting schema:")
    sourceDF.dataFrame.printSchema()

    val tokenRangeAccumulator =
      if (!sourceDF.savepointsSupported) None
      else {
        val tokenRangeAccumulator = TokenRangeAccumulator.empty
        spark.sparkContext.register(tokenRangeAccumulator, "Token ranges copied")

        addUSR2Handler(migratorConfig, tokenRangeAccumulator)
        startSavepointSchedule(scheduler, migratorConfig, tokenRangeAccumulator)

        Some(tokenRangeAccumulator)
      }

    log.info(
      "We need to transfer: " + sourceDF.dataFrame.rdd.getNumPartitions + " partitions in total")

    if (migratorConfig.source.isInstanceOf[SourceSettings.Cassandra]) {
      val partitions = sourceDF.dataFrame.rdd.partitions
      val cassandraPartitions = partitions.map(p => {
        p.asInstanceOf[CassandraPartition[_, _]]
      })
      var allTokenRanges = Set[(Token[_], Token[_])]()
      cassandraPartitions.foreach(p => {
        p.tokenRanges
          .asInstanceOf[Vector[CqlTokenRange[_, _]]]
          .foreach(tr => {
            val range =
              Set((tr.range.start.asInstanceOf[Token[_]], tr.range.end.asInstanceOf[Token[_]]))
            allTokenRanges = allTokenRanges ++ range
          })

      })

      log.info("All token ranges extracted from partitions size:" + allTokenRanges.size)

      if (migratorConfig.skipTokenRanges != None) {
        log.info(
          "Savepoints array defined, size of the array: " + migratorConfig.skipTokenRanges.size)

        val diff = allTokenRanges.diff(migratorConfig.skipTokenRanges)
        log.info("Diff ... total diff of full ranges to savepoints is: " + diff.size)
        log.debug("Dump of the missing tokens: ")
        log.debug(diff)
      }
    }

    log.info("Starting write...")

    try {
      migratorConfig.target match {
        case target: TargetSettings.Scylla =>
          writers.Scylla.writeDataframe(
            target,
            migratorConfig.renames,
            sourceDF.dataFrame,
            sourceDF.timestampColumns,
            tokenRangeAccumulator)
        case target: TargetSettings.Astra =>
          writers.Astra.writeDataframe(
            target,
            migratorConfig.renames,
            sourceDF.dataFrame,
            sourceDF.timestampColumns,
            tokenRangeAccumulator)
      }
    } catch {
      case NonFatal(e) => // Catching everything on purpose to try and dump the accumulator state
        log.error(
          "Caught error while writing the DataFrame. Will create a savepoint before exiting",
          e)
    } finally {
      tokenRangeAccumulator.foreach(dumpAccumulatorState(migratorConfig, _, "final"))
      scheduler.shutdown()
      spark.stop()
    }
  }

  def savepointFilename(path: String): String =
    s"${path}/savepoint_${System.currentTimeMillis / 1000}.yaml"

  def dumpAccumulatorState(config: MigratorConfig,
                           accumulator: TokenRangeAccumulator,
                           reason: String): Unit = {
    val filename =
      Paths.get(savepointFilename(config.savepoints.path)).normalize
    val rangesToSkip = accumulator.value.get.map(range =>
      (range.range.start.asInstanceOf[Token[_]], range.range.end.asInstanceOf[Token[_]]))

    val modifiedConfig = config.copy(
      skipTokenRanges = config.skipTokenRanges ++ rangesToSkip
    )

    Files.write(filename, modifiedConfig.render.getBytes(StandardCharsets.UTF_8))

    log.info(
      s"Created a savepoint config at ${filename} due to ${reason}. Ranges added: ${rangesToSkip}")
  }

  def startSavepointSchedule(svc: ScheduledThreadPoolExecutor,
                             config: MigratorConfig,
                             acc: TokenRangeAccumulator): Unit = {
    val runnable = new Runnable {
      override def run(): Unit =
        try dumpAccumulatorState(config, acc, "schedule")
        catch {
          case e: Throwable =>
            log.error("Could not create the savepoint. This will be retried.", e)
        }
    }

    log.info(
      s"Starting savepoint schedule; will write a savepoint every ${config.savepoints.intervalSeconds} seconds")

    svc.scheduleAtFixedRate(runnable, 0, config.savepoints.intervalSeconds, TimeUnit.SECONDS)
  }

  def addUSR2Handler(config: MigratorConfig, acc: TokenRangeAccumulator) = {
    log.info(
      "Installing SIGINT/TERM/USR2 handler. Send this to dump the current progress to a savepoint.")

    val handler = new SignalHandler {
      override def handle(signal: Signal): Unit =
        dumpAccumulatorState(config, acc, signal.toString)
    }

    Signal.handle(new Signal("USR2"), handler)
    Signal.handle(new Signal("TERM"), handler)
    Signal.handle(new Signal("INT"), handler)
  }
}
