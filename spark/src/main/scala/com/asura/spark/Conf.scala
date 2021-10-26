package com.asura.spark

import com.asura.spark.Conf.ConfigEntry

import java.util.Properties

/**
 * @author asura7969
 * @create 2021-10-25-0:01
 */

class Conf {

  private lazy val configuration = new Properties()

  def set(key: String, value: String): Conf = {
    configuration.setProperty(key, value)
    this
  }

  def set(key: ConfigEntry, value: String): Conf = {
    configuration.setProperty(key.key, value)
    this
  }

  def get(key: String, defaultValue: String): String = {
    Option(configuration.getProperty(key).asInstanceOf[String]).getOrElse(defaultValue)
  }

  def getOption(key: String): Option[String] = {
    Option(configuration.getProperty(key).asInstanceOf[String])
  }

  def getUrl(key: String): Object = {
    configuration.getProperty(key)
  }

  def get(t: ConfigEntry): String = {
    Option(configuration.getProperty(t.key).asInstanceOf[String]).getOrElse(t.defaultValue)
  }
}

object Conf {
  case class ConfigEntry(key: String, defaultValue: String)

  val ASURA_SPARK_ENABLED = ConfigEntry("asura.spark.enabled", "true")

  val ASURA_REST_ENDPOINT = ConfigEntry("asura.rest.address", "localhost:21000")

  val BLOCKING_QUEUE_CAPACITY = ConfigEntry("asura.blockQueue.size", "10000")
  val BLOCKING_QUEUE_PUT_TIMEOUT = ConfigEntry("asura.blockQueue.putTimeout.ms", "3000")

  val CLIENT_TYPE = ConfigEntry("asura.client.type", "kafka")
  val CLIENT_USERNAME = ConfigEntry("asura.client.username", "admin")
  val CLIENT_PASSWORD = ConfigEntry("asura.client.password", "admin123")
  val CLIENT_NUM_RETRIES = ConfigEntry("asura.client.numRetries", "3")

  val CLUSTER_NAME = ConfigEntry("asura.cluster.name", "primary")
}
