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

import scala.collection.immutable.SortedSet

import java.io.File

import org.json4s.JValue

import cats.data.{ Validated, ValidatedNel }
import cats.instances.list._
import cats.data.Validated._
import cats.syntax.traverse._
import cats.syntax.validated._
import cats.syntax.either._

import com.snowplowanalytics.iglu.client.Resolver

import generated.ProjectMetadata
import Utils.StringEnum

object Main extends App {

  import scopt.Read

  implicit val optionalStepRead =
    Read.reads { (Utils.fromString[OptionalWorkStep](_)).andThen(_.right.get) }

  implicit val skippableStepRead =
    Read.reads { (Utils.fromString[SkippableStep](_)).andThen(_.right.get) }

  sealed trait OptionalWorkStep extends StringEnum
  case object Compupdate extends OptionalWorkStep { def asString = "compupdate" }
  case object Vacuum extends OptionalWorkStep { def asString = "vacuum" }

  sealed trait SkippableStep extends StringEnum
  case object ArchiveEnriched extends SkippableStep { def asString = "archive_enriched" }
  case object Download extends SkippableStep { def asString = "download" }
  case object Analyze extends SkippableStep { def asString = "analyze" }
  case object Delete extends SkippableStep { def asString = "delete" }
  case object Shred extends SkippableStep { def asString = "shred" }
  case object Load extends SkippableStep { def asString = "load" }

  // TODO: this probably should not contain `File`s
  case class CliConfig(
    config: File,
    targetsDir: File,
    resolver: File,
    include: Seq[OptionalWorkStep],
    skip: Seq[SkippableStep],
    b64config: Boolean)

  case class AppConfig(
    configYaml: Config,
    b64config: Boolean,
    targets: Set[Targets.StorageTarget],
    include: SortedSet[OptionalWorkStep],
    skip: SortedSet[SkippableStep]) // Contains parsed configs

  def loadResolver(resolverConfig: File): Validated[ConfigError, Resolver] = {
    if (!resolverConfig.isFile) ParseError(s"[${resolverConfig.getAbsolutePath}] is not a file").invalid
    else if (!resolverConfig.canRead) ParseError(s"Resolver config [${resolverConfig.getAbsolutePath} is not readable").invalid
    else {
      import Compat._
      val json = readFile(resolverConfig).flatMap(Targets.safeParse).toValidatedNel

      def myParse(json: JValue): ValidatedNel[ConfigError, Resolver] =  // This should be ValidatedNel[ConfigError, _]
        Resolver.parse(json).fold(
          messages => Invalid(nel(messages.map(m => ValidationError(m)))),
          resolver => Valid(resolver)
        )

      val resolverZ: ValidatedNel[ConfigError, Resolver] =
        json.andThen(myParse)


      val f: ValidatedNel[String, Int] = ???

      ???
    }

    ???
  }

  def transform(cliConfig: CliConfig): Either[String, AppConfig] = {
    val resolver = loadResolver(cliConfig.resolver)
    val targets = loadTargetsFromDir(cliConfig.targetsDir, ???)

    // validatednel config
    // with
    // validatednel resolver or targets

//    for {
//      config <- readFile(cliConfig.config)
//
//    } ???
//    ???
    "".asLeft
  }

  val parser = new scopt.OptionParser[CliConfig]("scopt") {
    head("Relational Database Loader", ProjectMetadata.version)

    opt[File]('c', "config").required().valueName("<file>").
      action((x, c) ⇒ c.copy(config = x)).
      text("configuration file")

    opt[File]('t', "targets").required().valueName("<dir>").
      action((x, c) => c.copy(targetsDir = x)).
      text("directory with storage targets configuration JSONs")

    opt[Unit]('b', "base64-config-string").action((_, c) ⇒
      c.copy(b64config = true)).text("base64-encoded configuration string")

    opt[Seq[OptionalWorkStep]]('i', "include").action((x, c) ⇒
      c.copy(include = x)).text("include optional work steps")

    opt[Seq[SkippableStep]]('s', "skip").action((x, c) =>
      c.copy(skip = x)).text("skip steps")

    help("help").text("prints this usage text")

  }

  println("Hello Relational Database Loader!")
}
