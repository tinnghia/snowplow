/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.rdbloader
package loaders

import java.util.Properties
import java.io.FileReader
import java.sql.{SQLException, SQLTimeoutException}
import java.nio.file._

import scala.annotation.tailrec
import scala.collection.convert.wrapAsScala._

import cats.Semigroup
import cats.instances.all._
import cats.syntax.semigroup._
import cats.syntax.either._

import org.postgresql.Driver
import org.postgresql.jdbc.PgConnection
import org.postgresql.copy.CopyManager

import Targets.PostgresqlConfig
import Main.{Analyze, OptionalWorkStep, SkippableStep, Vacuum}

object PostgresqlLoader {

  val EventFiles = "part-*"
  val EventFieldSeparator = "\t"
  val NullString = ""
  val QuoteChar = "\\x01"
  val EscapeChar = "\\x02"

  case class PostgresLoadError(file: Path, exception: Exception)
  case class PostgresQueryError(query: String, exception: Exception)

  def getTable(schema: String): String = schema + ".events"

  /**
    *
    * @param folder local folder with downloaded events
    * @param target
    * @param tracking
    */
  def loadEvents(folder: String,
                 target: PostgresqlConfig,
                 skipSteps: Set[OptionalWorkStep],
                 skippableStep: Set[SkippableStep],
                 tracking: Boolean): Unit = {
    val eventFiles = getEventFiles(Paths.get(folder))
    val copyResult = copyViaStdin(target, eventFiles)

    copyResult match {
      case Left(error) =>
        Monitoring.trackLoadFailed(error)
      case Right(_) =>
        Monitoring.trackLoadSucceeded()
        getPostprocessingStatement(target, skipSteps, skippableStep) match {
          case Some(statement) =>
            // TODO: this should be handled
            executeQueries(target, List(statement)).map(_.toLong)
          case None => ()
        }
    }
  }

  /**
    * Without trailing space
    * @param skipSteps
    * @param skippableStep
    * @return
    */
  def getPostprocessingStatement(target: PostgresqlConfig, skipSteps: Set[OptionalWorkStep], skippableStep: Set[SkippableStep]): Option[String] = {
    val statements = List(skipSteps.find(_ == Vacuum), skippableStep.find(_ == Analyze)).flatten.map(_.asString.toUpperCase)
    statements match {
      case Nil => None
      case steps => Some(s"${steps.mkString(" ")} ${getTable(target.schema)};")
    }
  }

  def executeQueries(target: PostgresqlConfig, queries: List[String]) = {
    val conn = getConnection(target)
    val result = shortCircuit(queries, 0, executeQuery(conn))
    conn.close()
    result
  }

  def executeQuery(connection: PgConnection)(sql: String): Either[PostgresQueryError, Int] = {
    try {
      connection.createStatement().executeUpdate(sql).asRight
    } catch {
      case e: SQLException => PostgresQueryError(sql, e).asLeft
      case e: SQLTimeoutException => PostgresQueryError(sql, e).asLeft
    }
  }

  def atomicEventsMatcher(root: Path): PathMatcher =
    FileSystems.getDefault.getPathMatcher(s"glob:${root.toAbsolutePath.toString}/*/atomic-events/part-*");

  /**
    * It will match all atomic-event part-files shredded archive
    *
    * @param root
    * @return
    */
  def getEventFiles(root: Path): List[Path] = {
    val matcher = atomicEventsMatcher(root)
    def go(folder: Path, deep: Int = 2): List[Path] =
      Files.newDirectoryStream(folder).toList.flatMap { path =>
        if (Files.isDirectory(path) && deep > 0) go(path, deep - 1)
        else if (matcher.matches(path.toAbsolutePath)) List(path)
        else Nil
      }

    go(root)
  }

  def copyViaStdin(target: PostgresqlConfig, files: List[Path]): Either[PostgresLoadError, Long] = {
    val eventsTable = getTable(target.schema)
    val copyStatement = s"COPY $eventsTable FROM STDIN WITH CSV ESCAPE E'$EscapeChar' QUOTE E'$QuoteChar' DELIMITER '$EventFieldSeparator' NULL '$NullString'"

    val conn = getConnection(target)
    conn.setAutoCommit(false)
    val copyManager = new CopyManager(conn)

    val result = shortCircuit(files, 0L, copyIn(copyManager, copyStatement))
    if (result.isLeft) conn.rollback() else conn.commit()
    conn.close()
    result
  }

  def copyIn(copyManager: CopyManager, copyStatement: String)(file: Path): Either[PostgresLoadError, Long] = {
    try {
      copyManager.copyIn(copyStatement, new FileReader(file.toFile)).asRight
    } catch {
      case e: SQLException => PostgresLoadError(file, e).asLeft
      case e: SQLTimeoutException => PostgresLoadError(file, e).asLeft
    }
  }


  /**
    * Copy list of files into Redshift.
    * Short-circuit on first `Left` value of `process`
    *
    * @param list
    * @return total number of loaded rows in case of success
    *         or first error in case of failure
    */
  def copyInFiles(list: List[String], process: String => Either[PostgresLoadError, Long]): Either[PostgresLoadError, Long] = {
    @tailrec def go(files: List[String], itemsLoaded: Long): Either[PostgresLoadError, Long] = files match {
      case Nil => itemsLoaded.asRight
      case file :: remain => process(file) match {
        case Right(count) => go(remain, count + itemsLoaded)
        case Left(error) => error.asLeft
      }
    }

    go(list, 0L)
  }

  // Traverse all the things!!!
  def shortCircuit[I, L, R: Semigroup](items: List[I], init: R, process: I => Either[L, R]): Either[L, R] = {
    @tailrec def go(files: List[I], accumulated: R): Either[L, R] = files match {
      case Nil => accumulated.asRight
      case head :: tail => process(head) match {
        case Right(result) => go(tail, result |+| accumulated)
        case Left(error) => error.asLeft
      }
    }

    go(items, init)
  }


  def getConnection(target: PostgresqlConfig): PgConnection = {
    val url = s"jdbc:postgresql://${target.host}:${target.port}/${target.database}"

    val props = new Properties()
    props.setProperty("user", target.username)
    props.setProperty("password", target.password)
    props.setProperty("sslmode", target.sslMode.asProperty)
    props.setProperty("tcpKeepAlive", "true")

    new Driver().connect(url, props).asInstanceOf[PgConnection]
  }
}
