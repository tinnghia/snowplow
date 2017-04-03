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

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import Targets.RedshiftConfig
import Config.{GzipCompression, NoneCompression, OutputCompression, SnowplowAws}
import Main.{Compupdate, OptionalWorkStep, Shred, SkippableStep}

object RedshiftLoader {

  val EventFiles = "part-*"
  val EventFieldSeparator = "\t"
  val NullString = ""
  val QuoteChar = "\\x01"
  val EscapeChar = "\\x02"

  case class SqlStatement(copy: String, analyze: String, vacuum: String)

  def loadEventsAndShreddedTypes(config: Config, target: RedshiftConfig, skippableSteps: Set[SkippableStep]): Unit = {
    val awsCredentials = new BasicAWSCredentials(config.aws.accessKeyId, config.aws.secretAccessKey)

    val s3 = AmazonS3ClientBuilder
      .standard()
      .withRegion(config.aws.s3.region)
      .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
      .build()

    val shreddedStatements = getShreddedStatements(config, target, skippableSteps, s3)
  }

  def getShreddedStatements(config: Config, targets: RedshiftConfig, skippableSteps: Set[SkippableStep], s3: AmazonS3): List[SqlStatement] = {
    if (skippableSteps.contains(Shred)) {
      Nil
    } else {

    }
  }

  def getManifestStatements(): Unit = ???

  def buildCopyFromTsvStatement(config: Config, target: RedshiftConfig, include: Set[OptionalWorkStep]): String = {
    val credentials = getCredentials(config.aws)
    val compressionFormat = getCompressionFormat(config.enrich.outputCompression)
    val tableName = Common.getTable(target.schema)
    val comprows = if (include.contains(Compupdate)) s"COMPUPDATE COMPROWS ${target.compRows}" else ""

    // TODO: config.enrich is incorrect!
    s"""COPY $tableName FROM '${config.enrich}'
       | CREDENTIALS '$credentials' REGION AS '${config.aws.s3.region}'
       | DELIMITER '$EventFieldSeparator' MAXERROR ${target.maxError}
       | EMPTYASNULL FILLRECORD TRUNCATECOLUMNS $comprows
       | TIMEFORMAT 'auto' ACCEPTINVCHARS $compressionFormat;"""
      .stripMargin
  }

  def getCompressionFormat(outputCodec: OutputCompression): String = outputCodec match {
    case NoneCompression => ""
    case GzipCompression => "GZIP"
  }

  def getCredentials(aws: SnowplowAws): String =
    s"aws_access_key_id=${aws.accessKeyId};aws_secret_access_key=${aws.secretAccessKey}"


}
