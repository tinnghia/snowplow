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

import Targets.RedshiftConfig
import Config.SnowplowAws

object RedshiftLoader {

  val EventFiles = "part-*"
  val EventFieldSeparator = "\t"
  val NullString = ""
  val QuoteChar = "\\x01"
  val EscapeChar = "\\x02"


  def loadEventsAndShreddedTypes(target: RedshiftConfig): Unit = {
    ???
  }

  def getShreddedStatements(): Unit = ???

  def getManifestStatements(): Unit = ???

  def buildCopyFromTsvStatement(): Unit = {
    val creds = getCredentials()
    val steps
  }

  def getCredentials(aws: SnowplowAws): String =
    s"aws_access_key_id=${aws.accessKeyId};aws_secret_access_key=${aws.secretAccessKey}"


}
