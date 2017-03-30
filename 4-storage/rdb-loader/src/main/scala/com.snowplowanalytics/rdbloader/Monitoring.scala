package com.snowplowanalytics.rdbloader

import loaders.PostgresqlLoader.PostgresLoadError

object Monitoring {
  def trackLoadSucceeded(): Unit = {
    println("Successfuly loaded")
  }
  def trackLoadFailed(error: PostgresLoadError): Unit = {
    println("Errot during load")
  }
}
