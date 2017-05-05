/* 
 * Copyright (c) 2014-2017 Snowplow Analytics Ltd. All rights reserved.
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
import sbt._
import Keys._

object SnowplowRelationalDatabaseShredderBuild extends Build {

  import Dependencies._
  import BuildSettings._

  // Configure prompt to show current project
  override lazy val settings = super.settings :+ {
    shellPrompt := { s => Project.extract(s).currentProject.id + " > " }
  }

  // Define our project, with basic project information and library dependencies
  lazy val project = Project("snowplow-relational-database-shredder", file("."))
    .settings(buildSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        // Java
        Libraries.hadoopCommon,
        Libraries.hadoopClientCore,
        Libraries.cascadingCore,
        Libraries.cascadingLocal,
        Libraries.cascadingHadoop,
        Libraries.jsonValidator,
        Libraries.yodaTime,
        Libraries.yodaConvert,
        Libraries.dynamodb,
        // Scala
        Libraries.json4sJackson,
        Libraries.commonEnrich,
        Libraries.scaldingCore,
        Libraries.scaldingArgs,
        Libraries.scalaz7,
        Libraries.igluClient,
        // Scala (test only)
        Libraries.specs2,
        Libraries.scalazSpecs2,
        // Hadoop (test only)
        Libraries.hadoopClientCommon

      )
    )
}
