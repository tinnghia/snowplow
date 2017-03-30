package com.snowplowanalytics.rdbloader

import loaders.PostgresqlLoader.PostgresLoadError

object Monitoring {
  def trackLoadSucceeded(): Unit = {
    println("Successfully loaded")
  }
  def trackLoadFailed(error: PostgresLoadError): Unit = {
    println("Error during load")
  }
}
