package com.snowplowanalytics.rdbloader

import java.io.File

import scala.util.control.NonFatal

import cats.data.ValidatedNel
import cats.syntax.either._
import cats.syntax.validated._

import io.circe.{Json, ParsingFailure}
import io.circe.Decoder._
import io.circe.generic.auto._

import org.json4s.JValue
import org.json4s.jackson.{parseJson => parseJson4s}

import com.github.fge.jsonschema.core.report.ProcessingMessage

// Iglu client
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.validation.ValidatableJValue._

import Utils._
import Compat._

object Targets {

  sealed trait SslMode extends StringEnum
  case object Disable extends SslMode { def asString = "DISABLE" }
  case object Require extends SslMode { def asString = "REQUIRE" }
  case object VerifyCa extends SslMode { def asString = "VERIFY_CA" }
  case object VerifyFull extends SslMode { def asString = "VERIFY_FULL" }

  sealed trait Purpose extends StringEnum
  case object DuplicateTracking extends Purpose { def asString = "DUPLICATE_TRACKING" }
  case object FailedEvents extends Purpose { def asString = "FAILED_EVENTS" }
  case object EnrichedEvents extends Purpose { def asString = "ENRICHED_EVENTS" }

  implicit val sslModeDecoder =
    decodeStringEnum[SslMode]

  implicit val purposeDecoder =
    decodeStringEnum[Purpose]

  sealed trait StorageTarget extends Product with Serializable {
    def name: String
    def purpose: Purpose
  }

  def safeParse(target: String): Either[ConfigError, JValue] =
    try {
      parseJson4s(target).asRight
    } catch {
      case NonFatal(e) => ParseError(ParsingFailure("Invalid storage target JSON", e)).asLeft
    }


  def loadFromFile(resolver: Resolver)(file: File): ValidatedNel[ConfigError, StorageTarget] = {
    val content = readFile(file).toValidatedNel
    content.andThen(parseTarget(resolver))
  }


  def validate(resolver: Resolver)(json: JValue): ValidatedNel[ConfigError, Json] = {
    val result: ValidatedNel[ProcessingMessage, JValue] = json.validate(dataOnly = false)(resolver)
    result.map(Compat.jvalueToCirce).leftMapNel(ValidationError)  // Convert from Iglu client's format, TODO compat
  }


  def loadTargetsFromDir(directory: File, resolver: Resolver): ValidatedNel[ConfigError, List[Targets.StorageTarget]] = {
    if (!directory.isDirectory) ParseError(s"[${directory.getAbsolutePath}] is not a directory").invalidNel
    else if (!directory.canRead) ParseError(s"Targets directory [${directory.getAbsolutePath} is not readable").invalidNel
    else {
      val fileList = directory.listFiles.toList
      val targetsList = fileList.map(Targets.loadFromFile(resolver))
      targetsList.sequenceU
    }
  }


  def parseTarget(resolver: Resolver)(target: String): ValidatedNel[ConfigError, StorageTarget] = {
    val json = safeParse(target).toValidatedNel
    val validatedJson = json.andThen(validate(resolver))
    validatedJson.andThen(decodeStorageTarget(_).toValidatedNel)
  }


  def decodeStorageTarget(validJson: Json): Either[DecodingError, StorageTarget] = {
    val nameDataPair = for {
      jsonObject <- validJson.asObject
      schema <- jsonObject.toMap.get("schema")
      data <- jsonObject.toMap.get("data")
      schemaKey <- schema.asString
      key <- SchemaKey.fromUri(schemaKey)
    } yield (key.name, data)

    nameDataPair match {
      case Some(("elastic_config", data)) => data.as[ElasticConfig].leftMap(DecodingError.apply)
      case Some(("amazon_dynamodb_config", data)) => data.as[AmazonDynamodbConfig].leftMap(DecodingError.apply)
      case Some(("postgresql_config", data)) => data.as[PostgresqlConfig].leftMap(DecodingError.apply)
      case Some(("redshift_config", data)) => data.as[RedshiftConfig].leftMap(DecodingError.apply)
      case Some((name, _)) => DecodingError(s"Unknown storage target [$name]").asLeft
      case None => DecodingError("Not a self-describing JSON was used as storage target configuration").asLeft
    }
  }

  case class ElasticConfig(
      name: String,
      host: String,
      index: String,
      port: Int,
      `type`: String,
      nodesWanOnly: Boolean)
    extends StorageTarget {
    val purpose = FailedEvents
  }

  case class AmazonDynamodbConfig(
      name: String,
      accessKeyId: String,
      secretAccessKey: String,
      awsRegion: String,
      dynamodbTable: String)
    extends StorageTarget {
    val purpose = DuplicateTracking
  }

  case class PostgresqlConfig(
      name: String,
      host: String,
      database: String,
      port: Int,
      sslMode: SslMode,
      schema: String,
      username: String,
      password: String)
    extends StorageTarget {
    val purpose = EnrichedEvents
  }

  case class RedshiftConfig(
      name: String,
      host: String,
      database: String,
      port: Int,
      sslMode: SslMode,
      schema: String,
      username: String,
      password: String,
      maxError: Int,
      compRows: Long)
    extends StorageTarget {
    val purpose = EnrichedEvents
  }
}
