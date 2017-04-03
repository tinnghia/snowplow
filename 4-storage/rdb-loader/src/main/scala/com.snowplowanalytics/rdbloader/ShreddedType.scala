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

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result}

import scala.collection.convert.wrapAsScala._
import scala.collection.mutable.ListBuffer

class ShreddedType(s3ObjectPath: String, schema: String) {
  val parts = "^.*/(?<vendor>[^/]+)/(?<name>[^/]+)/(?<format>[^/]+)/(?<version_model>[^/]+)-$".r

  val schemaPathRegex = (
    "^([a-zA-Z0-9-_.]+)/" +
      "([a-zA-Z0-9-_]+)/" +
      "([a-zA-Z0-9-_]+)/" +
      "([1-9][0-9]*" +
      "(?:-(?:0|[1-9][0-9]*)){2})$").r

}

object ShreddedType {
  def discoverShreddedTypes(s3: AmazonS3, s3Path: String, schema: String): List[ShreddedType] = {
    def splitS3Path(fullPath: String): (String, String) = ???

    val (bucket, path) = splitS3Path(s3Path)
    val objects = listS3(s3, bucket, path).filterNot(inAtomicEvents)

    ???
  }

  def create(bucket: String)(key: String): String =
    s"s3://$bucket/"

  def getSchemaVer(s: String): String = {
    val segments = s.split("/")
    val path = segments.init
    val model = segments.last.split("-").head
    s"${path.mkString("/")}/$model-"
  }

  def inAtomicEvents(key: String): Boolean =
    key.split("/").contains("atomic-events")    // TODO: filter it in `listS3`

  /**
    * Helper method to get **all** keys from bucket.
    * Unlike usual `s3.listObjects` it won't truncate response after 1000 items
    * TODO: check if we really can reach 1000 items
    *
    * @param s3
    * @param bucket
    * @param prefix
    * @return
    */
  private def listS3(s3: AmazonS3, bucket: String, prefix: String): List[String] = {
    var result: ListObjectsV2Result = null
    val buffer = ListBuffer.empty[String]
    val req  = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix)

    do {
      result = s3.listObjectsV2(req)
      val objects = result.getObjectSummaries.map(_.getKey)
      buffer.appendAll(objects)
    } while (result.isTruncated)
    buffer.toList
  }
}