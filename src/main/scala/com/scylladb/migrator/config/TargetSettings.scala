package com.scylladb.migrator.config

import cats.implicits._
import io.circe.{ Decoder, DecodingFailure, Encoder, Json }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }
import io.circe.syntax._

sealed trait TargetSettings
object TargetSettings {
  case class Scylla(cloudconnectionbuild: String,
                    host: String,
                    port: Int,
                    localDC: Option[String],
                    credentials: Option[Credentials],
                    sslOptions: Option[SSLOptions],
                    keyspace: String,
                    table: String,
                    connections: Option[Int],
                    stripTrailingZerosForDecimals: Boolean,
                    writeTTLInS: Option[Int],
                    writeWritetimestampInuS: Option[Long])
      extends TargetSettings
  
  case class Astra(host: Option[String],
                  port: Option[Int],
                  credentials: Option[Credentials],
                  keyspace: Option[String],
                  table: Option[String],
                  bundlePath: Option[String],
                  database: Option[String],
                  connections: Option[Int],
                  stripTrailingZerosForDecimals: Boolean,
                  writeTTLInS: Option[Int],
                  writeWritetimestampInuS: Option[Long])
      extends TargetSettings

  implicit val decoder: Decoder[TargetSettings] =
    Decoder.instance { cursor =>
      cursor.get[String]("type").flatMap {
        case "scylla" | "cassandra" =>
          deriveDecoder[Scylla].apply(cursor)
        case "astra" =>
          deriveDecoder[Astra].apply(cursor)
        case otherwise =>
          Left(DecodingFailure(s"Invalid target type: ${otherwise}", cursor.history))
      }
    }

  implicit val encoder: Encoder[TargetSettings] =
    Encoder.instance {
      case t: Scylla =>
        deriveEncoder[Scylla].encodeObject(t).add("type", Json.fromString("scylla")).asJson

      case t: Astra =>
        deriveEncoder[Astra].encodeObject(t).add("type", Json.fromString("astra")).asJson
    }
}
